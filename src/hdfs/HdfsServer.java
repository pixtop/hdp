package hdfs;

import java.lang.*;
import java.net.*;
import java.io.*;
import java.util.Scanner;

// Serveur hdfs à lancer sur chaque machine, lance un dataNode et optionnellement un nameNode
public class HdfsServer {

  private static int port = 4242;
  private static String usage = "Usage : java HdfsServer [nameNode] [-f saved_config_file]";
  private static String config_file_output = "HdfsServer.conf";
  private static NameNode name;

  // Thread gerant le traitement de la demande d'un client
  public static class Traitement implements Runnable {

    private Socket socket;

    public Traitement (Socket socket) {
      this.socket = socket;
    }

    public void run() {
      InetAddress remote = ((InetSocketAddress)this.socket.getRemoteSocketAddress()).getAddress();
      System.out.println("Connexion received from " + remote.getHostAddress());

      // Traitement TODO

      try {
        this.socket.close();
      } catch (IOException e) {
        System.err.println("Error closing socket : " + e.getMessage());
      }
    }

  }

  // Lancer le seveur hdfs
  // Lance un dataNode et un nameNode en plus si l'arg nameNode est passé en ligne de commande
  public static void main(String[] args) {

    HdfsServer.name = null; // NameNode si on a spécifié l'option -n
    DataNode data; // DataNode
    boolean is_data = false; // Si on a spécifié l'option -n
    InetAddress name_addr = null; // Adresse du NameNode
    SlaveKeepAlive keepalive; // keepAlive du NameNode

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
          System.err.println("Unknown option " + args[i]);
          System.exit(1);
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
    // TODO Lancer le DataNode
    System.out.println("DataNode started");

    // Socket serveur à l'ecoute de client
    ServerSocket serv_socket = null;
    try {
      serv_socket = new ServerSocket(HdfsServer.port);
    } catch (IOException e) {
      System.err.println("Port" + HdfsServer.port + " already in use");
      System.exit(1);
    }

    // Thread lance a l'arret du serveur -> Enregistrer les données(du NameNode) sur le disque
    Runtime run = Runtime.getRuntime();
    run.addShutdownHook(new Thread(){

      @Override
      public void run()
      {
        try {
          FileOutputStream file = new FileOutputStream(HdfsServer.config_file_output);
          ObjectOutputStream oos = new ObjectOutputStream(file);
          oos.writeObject(HdfsServer.name);
          oos.close();
          file.close();
          // méthodes sauvegardes de données du DataNode TODO
          System.err.println(" -> NameNode data saved to " + HdfsServer.config_file_output);
        } catch (Exception e) {
          System.err.println(" -> Error while saving : " + e.getMessage());
          System.err.println("Shutdown without saving data");
        }
      }

    });

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
