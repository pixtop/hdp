package ordo;

import config.Project;

import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/** Serveur Hidoop
* Permet de lancer un DaemonDataNode et optionellement un DaemonMonitor
* Show usage : java HidoopServer -h
*/
public class HidoopServer {

  private static DaemonDataNode ddn; // DaemonDataNode
  private static DaemonMonitor moniteur; // DaemonMonitor

  public static void usage() {
    System.out.println("Usage: java HidoopServer [root_directory] [-m] [-h <nameNode_host>]");
    System.out.println("root_directory : Répertoire où le dataNode range ses chunks, utilisé par le DaemonDataNode");
    System.out.println("-m : En plus du DaemonDataNode, lancer aussi un DaemonMonitor");
    System.out.println("-h <nameNode_host> : Adresse du nameNode, utilisée par le DaemonMonitor");
  }

  public static void main(String[] args) {
    String root = ".", nameNode = "localhost";
    boolean launch_monitor = false;
    for(int i = 0;i < args.length; i ++) {
      switch(args[i]) {
        case "help":
        case "-h":
          if(++i < args.length) {
            nameNode = args[i];
          } else {
            HidoopServer.usage();
            System.exit(0);
          }
          break;
        case "-m":
          launch_monitor = true;
          break;
        default:
          root = args[i];
      }
    }
    try {
      LocateRegistry.createRegistry(Project.RMI_PORT);
      HidoopServer.ddn = new DaemonDataNode(root);
      if(launch_monitor) {
        HidoopServer.moniteur = new DaemonMonitor(nameNode);
        Naming.rebind("//"+ InetAddress.getLocalHost().getHostAddress()+":"+Project.RMI_PORT+"/"+Project.RMI_MONITOR, HidoopServer.moniteur);
        System.out.println("Monitor launched");
      }
      Naming.rebind("//"+ InetAddress.getLocalHost().getHostAddress()+":"+Project.RMI_PORT+"/"+Project.RMI_DAEMON, HidoopServer.ddn);
      System.out.println("DaemonDataNode launched");
      HidoopServer.ddn.run();
    } catch (UnknownHostException e) {
      System.err.println("Error: host " + nameNode + " unknown");
      System.exit(1);
    } catch (RemoteException e) {
      System.err.println("Port " + Project.RMI_PORT + " already used");
      System.exit(1);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      System.exit(1);
    }
  }

}
