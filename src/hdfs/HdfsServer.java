package hdfs;

import java.lang.*;
import java.net.*;
import java.io.*;
import java.util.*;
import java.nio.file.NotDirectoryException;

import formats.*;
import exceptions.*;

// Serveur hdfs à lancer sur chaque machine, lance un dataNode et optionnellement un nameNode
public class HdfsServer {

  public static int port = 4242;
  private static String usage = "Usage : java HdfsServer [node_root] [-n] [-f saved_config_file]";
  private static String config_file_output = "HdfsServer.conf";
  private static NameNode name; // NameNode
  private static DataNode data; // DataNode
  private static String nodeRoot; // Root du serveur

  // Thread gerant le traitement de la demande d'un client
  public static class Traitement implements Runnable {

    private Socket socket;

    public Traitement (Socket socket) {
      this.socket = socket;
    }

    public void run() {

      // Création streams
      ObjectOutputStream oos = null;
      ObjectInputStream ois = null;
      try {
        oos = new ObjectOutputStream(this.socket.getOutputStream());
        ois = new ObjectInputStream(this.socket.getInputStream());
      } catch (IOException e) {
        System.err.println("Error while opening socket : " + e.getMessage());
      }

      // Affichage nouvelle connexion
      InetAddress remote = ((InetSocketAddress)this.socket.getRemoteSocketAddress()).getAddress();
      System.out.println("Connexion received from " + remote.getHostAddress());

      // Récupération requête
      HdfsQuery query = null;
      try {
        if(ois != null)query = (HdfsQuery)ois.readObject();
      } catch (StreamCorruptedException e) {
        System.err.println("Wrong message format error : " + e.getMessage());
      } catch (Exception e) {
        System.err.println("Message format error : " + e.getMessage());
      }

      // traitement requête
      if(query != null) {
        try {
          switch(query.getCmd()) {
            case GET_FILE:
              System.out.println(" |-> Request dataNodes handling file " + query.getName());
              if(HdfsServer.name != null) {
                InfoFichier info = HdfsServer.name.getInfoFichier(query.getName());
                if(info != null) {
                  oos.writeObject(new HdfsResponse(info.getChunks(), null));
                  System.out.println(" |-> Chunks returned");
                } else {
                  System.err.println(" |-> Error : File not found");
                  oos.writeObject(new HdfsResponse(null, new FileNotFoundException("File not found")));
                }
              } else {
                System.err.println(" |-> Error : Not a NameNode");
                oos.writeObject(new HdfsResponse(null, new NotANameNode("Not a NameNode")));
              }
              break;
            case GET_CHUNK:
              try {
                oos.writeObject(new HdfsResponse(HdfsServer.data.getChunk(query.getName(), query.getChunk()), null));
              } catch (FileNotFoundException e) {
                oos.writeObject(new HdfsResponse(null, e));
              }
              break;
            case GET_DATANODES:
              System.out.println(" |-> Request all dataNodes");
              if(HdfsServer.name != null) {
                oos.writeObject(new HdfsResponse(HdfsServer.name.getDataNodes(), null));
              } else {
                System.err.println(" |-> Error : Not a NameNode");
                oos.writeObject(new HdfsResponse(null, new NotANameNode("Not a NameNode")));
              }
              break;
            case WRT_FILE:
              System.out.println(" |-> Recording new file " + query.getName());
              if(HdfsServer.name != null) {
                InfoFichier nfile = new InfoFichier(query.getName());
                Map dataNodes = (Map)query.getData(); // ArrayList<Inet4Address>
                for(Object i : dataNodes.keySet()) {
                  nfile.addChunk((Integer)i, (Inet4Address)dataNodes.get((Integer)i));
                }
                try {
                  HdfsServer.name.ajouterFichier(nfile);
                  oos.writeObject(new HdfsResponse(null, null));
                } catch(AlreadyExists e) {
                  oos.writeObject(new HdfsResponse(null, e));
                }
              } else {
                System.err.println(" |-> Error : Not a NameNode");
                oos.writeObject(new HdfsResponse(null, new NotANameNode("Not a NameNode")));
              }
              break;
            case WRT_CHUNK:
              try {
                HdfsServer.data.addChunk(query.getName(), query.getChunk(), (String)query.getData());
                oos.writeObject(new HdfsResponse(null, null));
              } catch(IOException e) {
                oos.writeObject(new HdfsResponse(null, e));
              }
              break;
            case DEL_CHUNK:
              try {
                HdfsServer.data.delChunk(query.getName(), query.getChunk());
                oos.writeObject(new HdfsResponse(null, null));
              } catch(IOException e) {
                oos.writeObject(new HdfsResponse(null, e));
              }
              break;
            case DEL_FILE:
              System.out.println(" |-> Delete file " + query.getName());
              if(HdfsServer.name != null) {
                HdfsServer.name.supprimerFichier(query.getName());
              } else {
                System.err.println(" |-> Error : Not a NameNode");
                oos.writeObject(new HdfsResponse(null, new NotANameNode("Not a NameNode")));
              }
              break;
          }
        } catch (IOException e) {
          System.err.println("Error while writing socket : " + e.getMessage());
        }
      }

      try {
        if(oos != null)oos.close();
        if(ois != null)ois.close();
        this.socket.close();
      } catch (IOException e) {
        System.err.println("Error while closing socket : " + e.getMessage());
      }
    }

  }

  // Lancer le seveur hdfs
  // Lance un dataNode et un nameNode en plus si l'arg nameNode est passé en ligne de commande
  public static void main(String[] args) {

    HdfsServer.name = null; // NameNode si on a spécifié l'option -n
    boolean is_data = false; // Si on a spécifié l'option -n
    InetAddress name_addr = null; // Adresse du NameNode
    SlaveKeepAlive keepalive; // keepAlive du NameNode
    HdfsServer.nodeRoot = "."; // Path ou s'enregistrerons les fichiers

    // Traitement des arguments
    for(int i = 0;i < args.length; i ++) {
      switch(args[i]) {
        case "-h":
        case "--help":
          System.out.println(HdfsServer.usage);
          System.exit(0);
        case "nameNode":
        case "namenode":
        case "-n":
          is_data = true;
          break;
        case "-f":
          is_data = true;
          if(++i < args.length) {
            try {
              FileInputStream file = new FileInputStream(args[i]);
              ObjectInputStream ois = new ObjectInputStream(file);
              HdfsServer.name = (NameNode)ois.readObject();
              ois.close();
              file.close();
              System.out.println("NameNode started from config file");
            } catch (FileNotFoundException e) {
              System.err.println("File not found : " + e.getMessage());
              System.exit(1);
            } catch (EOFException e) {
              System.err.println("Wrong config file format");
              System.exit(1);
            } catch (StreamCorruptedException e) {
              System.err.println("Wrong config file format : " + e.getMessage());
              System.exit(1);
            } catch (Exception e) {
              System.err.println("Config file error : " + e.getMessage());
              System.exit(1);
            }
          } else {
            System.err.println("No file specified after the -f option");
            System.exit(1);
          }
        break;
        default:
          HdfsServer.nodeRoot = args[i];
      }
    }
    // Construction NameNode(si voulu et vaut null) + dataNode
    if(is_data) {
      // Création NameNode si pas de fichier de cofig spécifié
      if(HdfsServer.name == null) {
        HdfsServer.name = new NameNode();
        Scanner input = new Scanner(System.in);
        System.out.println("Enter addresses/hosts of others dataNodes - empty line to stop");
        String line = input.nextLine();
         while (!line.equals("")) {
          try {
            Inet4Address addr = (Inet4Address)Inet4Address.getByName(line);
            HdfsServer.name.addDataNode(addr);
          } catch (UnknownHostException e) {
            System.err.println("Unknown host " + e.getMessage());
            System.exit(1);
          }
          line = input.nextLine();
        }
        System.out.println("End of the list -> NameNode started");
      }
      // Récupération adresse locale
      try {
        name_addr = InetAddress.getLocalHost();
      } catch (UnknownHostException e) {
        System.err.println("Error looking for hostname : " + e.getMessage());
        System.exit(1);
      }
      HdfsServer.name.addDataNode((Inet4Address)name_addr);
      // Lancement du SlaveKeepAlive
      keepalive = new SlaveKeepAlive(HdfsServer.name);
  		keepalive.start();
      System.out.println("SlaveKeepAlive started (NameNode utility)");
    } else {
      Scanner input = new Scanner(System.in);
      System.out.println("Enter address/host of the NameNode");
      String line = input.nextLine();
      try {
        name_addr = Inet4Address.getByName(line);
      } catch (UnknownHostException e) {
        System.err.println("Unknown host " + e.getMessage());
        System.exit(1);
      }
    }
    // Lancement dataNode
    try {
      HdfsServer.data = new DataNode(HdfsServer.nodeRoot);
    } catch (NotDirectoryException e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
    System.out.println("DataNode started");

    // Thread lance a l'arret du serveur -> Enregistrer les données(du NameNode) sur le disque
    Runtime run = Runtime.getRuntime();
    run.addShutdownHook(new Thread(){

      @Override
      public void run()
      {
        try {
          if(HdfsServer.name != null) {
            FileOutputStream file = new FileOutputStream(HdfsServer.nodeRoot + '/' + HdfsServer.config_file_output);
            ObjectOutputStream oos = new ObjectOutputStream(file);
            oos.writeObject(HdfsServer.name);
            oos.close();
            file.close();
            System.out.println(" -> NameNode data saved to " + HdfsServer.config_file_output);
          } else System.out.println(" -> Shutdown completed");
          // méthodes sauvegardes de données du DataNode TODO
        } catch (Exception e) {
          System.err.println(" -> Error while saving : " + e.getMessage());
          System.err.println("Shutdown without saving data");
        }
      }

    });

    // Socket serveur à l'ecoute de client
    ServerSocket serv_socket = null;
    try {
      serv_socket = new ServerSocket(HdfsServer.port);
    } catch (IOException e) {
      System.err.println("Port" + HdfsServer.port + " already in use");
      System.exit(1);
    }

    // Attente de connexion
    while(true) {
      Socket socket = null;
      try {
        socket = serv_socket.accept();
      } catch (IOException e) {
        System.err.println("Error acceptance new client connexion : " + e.getMessage());
        continue;
      }
      Thread t = new Thread(new HdfsServer.Traitement(socket));
      t.start();
    }

  }

}
