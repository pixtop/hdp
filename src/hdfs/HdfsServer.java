package hdfs;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

import exceptions.AlreadyExists;
import exceptions.NotANameNode;
import formats.HdfsQuery;
import formats.HdfsResponse;

// Serveur hdfs à lancer sur chaque machine, lance un dataNode et optionnellement un nameNode
public class HdfsServer {

  public static int port = 4242;
  private static String usage = "Serveur lancant un dataNode et optionnellement un nameNode (option -n ou -f)\nUsage : java HdfsServer [node_root] [-n | -f saved_config_file] [-h nameNode_host]\nSi option --noData précisée, aucun dataNode n'est pas lancé";
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

    @Override
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
              System.out.print(" |-> GET_FILE request");
              if(HdfsServer.name != null) {
                if(query.getName() != null) { // Request for dataNodes handling a file
                  System.out.print(" for file " + query.getName() + "\n");
                  InfoFichier info = HdfsServer.name.getInfoFichier(query.getName());
                  if(info != null) {
                    oos.writeObject(new HdfsResponse(info, null));
                  } else {
                    System.err.println(" |-> Error : File not found");
                    oos.writeObject(new HdfsResponse(null, new FileNotFoundException("File not found")));
                  }
                } else { // Request for all files registered in the NameNode
                  System.out.print(" for the list of all files stored in NameNode\n");
                  if(HdfsServer.name.getNbFiles() > 0)
                    oos.writeObject(new HdfsResponse(HdfsServer.name.getAllFileNames(), null));
                  else
                    oos.writeObject(new HdfsResponse(new String[0], null));
                }
              } else {
                System.out.print("\n");
                System.err.println(" |-> Error : Not a NameNode");
                oos.writeObject(new HdfsResponse(null, new NotANameNode("Not a NameNode")));
              }
              break;
            case GET_CHUNK:
              if(query.getName() != null) {
                System.out.println(" |-> Request chunk of index " + query.getChunk() + " of file " + query.getName());
                try {
                  oos.writeObject(new HdfsResponse(HdfsServer.data.getChunk(query.getName(), query.getChunk()), null));
                } catch (FileNotFoundException e) {
                  System.err.println(" |-> Error : Chunk not found");
                  oos.writeObject(new HdfsResponse(null, e));
                }
              } else {
                System.out.println(" |-> Request all chunks stored in DataNode");
                oos.writeObject(new HdfsResponse(HdfsServer.data.showChunks(), null));
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
              InfoFichier f = query.getInfo();
              System.out.println(" |-> Recording new file " + f.getNom() + " of type " + f.getFormat());
              if(HdfsServer.name != null) {
                try {
                  HdfsServer.name.ajouterFichier(f);
                  oos.writeObject(new HdfsResponse(null, null));
                } catch(AlreadyExists e) {
                  System.err.println(" |-> Error : File already exists");
                  oos.writeObject(new HdfsResponse(null, e));
                }
              } else {
                System.err.println(" |-> Error : Not a NameNode");
                oos.writeObject(new HdfsResponse(null, new NotANameNode("Not a NameNode")));
              }
              break;
            case WRT_CHUNK:
              System.out.println(" |-> Adding chunk of file " + query.getName() + ", index : " + query.getChunk());
              try {
                HdfsServer.data.addChunk(query.getName(), query.getChunk(), (String)query.getData());
                oos.writeObject(new HdfsResponse(null, null));
              } catch(IOException e) {
                System.err.println(" |-> Error : Could'nt write chunk");
                oos.writeObject(new HdfsResponse(null, e));
              }
              break;
            case DEL_CHUNK:
              System.out.println(" |-> Deleting chunk of file " + query.getName() + ", index : " + query.getChunk());
              try {
                HdfsServer.data.delChunk(query.getName(), query.getChunk());
                oos.writeObject(new HdfsResponse(null, null));
              } catch(IOException e) {
                System.err.println(" |-> Error : Could'nt delete chunk");
                oos.writeObject(new HdfsResponse(null, e));
              }
              break;
            case DEL_FILE:
              System.out.println(" |-> Delete file " + query.getName());
              if(HdfsServer.name != null) {
                HdfsServer.name.supprimerFichier(query.getName());
                oos.writeObject(new HdfsResponse(null, null));
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
    boolean is_name = false; // Si on a spécifié l'option -n
    boolean is_data = true;
    InetAddress name_addr = null; // Adresse du NameNode
    SlaveKeepAlive keepalive; // keepAlive du NameNode
    HdfsServer.nodeRoot = "."; // Path ou s'enregistrerons les fichiers

    // Traitement des arguments
    for(int i = 0;i < args.length; i ++) {
      switch(args[i]) {
        case "-h":
          if(++i < args.length) {
            try {
              name_addr = Inet4Address.getByName(args[i]);
            } catch (UnknownHostException e) {
              System.err.println("Unknown host " + e.getMessage());
              System.exit(1);
            }
            continue;
          }
        case "--help":
          System.out.println(HdfsServer.usage);
          System.exit(0);
        case "nameNode":
        case "namenode":
        case "-n":
          is_name = true;
          break;
        case "-f":
          is_name = true;
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
        case "--noData":
          is_data = false;
        break;
        default:
          HdfsServer.nodeRoot = args[i];
      }
    }
    // Construction NameNode(si voulu et vaut null) + dataNode
    if(is_name) {
      // Création NameNode si pas de fichier de cofig spécifié
      if(HdfsServer.name == null) {
        HdfsServer.name = new NameNode();

        // Si on veut lancer le nameNode depuis un script de configuration avec les dataNodes enregistrés de base
        // interêt : ne pas attendre de recevoir le keepAlive des DataNodes avant de pouvoir commencer la manip
        /*
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
        } */
        System.out.println("NameNode started");
      }
      // Récupération adresse locale
      if(name_addr == null) {
        try {
          name_addr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
          System.err.println("Error looking for hostname : " + e.getMessage());
          System.exit(1);
        }
      }


    	  // Lancement du SlaveKeepAlive
        keepalive = new SlaveKeepAlive(HdfsServer.name);
      	keepalive.start();
        System.out.println("SlaveKeepAlive started (NameNode utility)");

    }
    if(is_data) {
      if(name_addr == null) {
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
        HdfsServer.data = new DataNode(HdfsServer.nodeRoot, (Inet4Address)name_addr);
      } catch (IOException e) {
        System.err.println("DataNode error : " + e.getMessage());
        System.exit(1);
      }
      System.out.println("DataNode started");
    }


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
