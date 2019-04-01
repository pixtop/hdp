package ordo;

import config.Project;
import formats.Format;
import map.MapReduce;

import java.io.IOException;
import java.io.File;
import java.nio.file.NotDirectoryException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.lang.System;


/**
 * DaemonDataNode, exécute les maps sur les fichiers du dataNode
 * Lancement : java DaemonDataNode [dossier_root(celui du dataNode)]
 * Port par défaut : voir dans config.Projet
 * Utiliser HidoopServer plutôt pour le lancer
*/
public class DaemonDataNode extends UnicastRemoteObject implements Daemon {

    private class Task {
        private MapReduce mr;
        private Format r, w;
        private CallBack cb;

        private Task(MapReduce mr, Format r, Format w, CallBack cb) {
            this.mr = mr;
            this.r = r;
            this.w = w;
            this.cb = cb;
        }
    }

    private final ArrayList<Task> mapQ;

    private File dir; // Dossier dans lequel créer les résultats des maps (même que celui du dataNode, obligatoire)

    public static int getIndex(String fileName) {
      String index = "";
      int i = fileName.length() - 1;
      char c = fileName.charAt(i);
      while(c >= '0' && c <= '9') {
        index = c + index;
        c = fileName.charAt(--i);
      }
      return Integer.parseInt(index);
    }

    public DaemonDataNode(String dir) throws RemoteException, NotDirectoryException {
        this.dir = new File(dir);

        if (!this.dir.exists() || !this.dir.isDirectory()) {
            throw new NotDirectoryException("Repertory " + dir + " does not exist");
        }

        mapQ = new ArrayList<>();
    }

    @Override
    public void runMap(MapReduce m, Format reader, Format writer, CallBack cb) {
        // Changer pour le bon répertoire de travail
        reader.setFname(this.dir.getPath() + "/" + reader.getFname());
        writer.setFname(this.dir.getPath() + "/" + writer.getFname());
        // Ajouter une tâche
        synchronized (mapQ) {
            mapQ.add(new Task(m, reader, writer, cb));
            mapQ.notify();
        }
    }

    /**
     * Run map task while there is one task in the queue.
     * Wait if no task left.
     */
    public void run() {
        Task task;
        while (true) {
            synchronized (mapQ) {
                if (mapQ.isEmpty()) {
                    try {
                        System.out.println("Waiting task..");
                        mapQ.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                task = mapQ.get(0);
                mapQ.remove(0);
            }
            System.out.println("Map in progress");
            long ctime = System.currentTimeMillis();
            try {
                task.r.open(Format.OpenMode.R);
                task.w.open(Format.OpenMode.W);
                task.mr.map(task.r, task.w);
            } catch (IOException e) {
                System.out.println("Error: Could not execute mapping");
            }
            task.r.close();
            task.w.close();
            ctime = System.currentTimeMillis() - ctime;
            try {
                task.cb.mapDone(DaemonDataNode.getIndex(task.r.getFname()), (double)ctime / 1000F);
            } catch (RemoteException e) {
                // Connexion error between DaemonDataNode and DaemonMonitor
                e.printStackTrace();
            }
            System.out.println("Map ended");
        }
    }

    static public void main(String[] args) {

        try {
          DaemonDataNode daemon = null;
          if(args.length > 1) {
            daemon = new DaemonDataNode(args[0]);
          }
          else daemon = new DaemonDataNode(".");

          LocateRegistry.createRegistry(Project.RMI_PORT);
          Naming.rebind("//"+ InetAddress.getLocalHost().getHostAddress()+":"+Project.RMI_PORT+"/"+Project.RMI_DAEMON, daemon);
          System.out.println("Daemon launched successfully");
          daemon.run();
        } catch (RemoteException e) {
          System.err.println("Port " + Project.RMI_PORT + " already used");
          System.exit(1);
        } catch (UnknownHostException | MalformedURLException e) {
          System.err.println("Unknown host error (should not happen)");
          System.exit(1);
        } catch (NotDirectoryException e) {
          System.err.println("Directory " + args[0] + " does not exist");
          System.exit(1);
        }
    }
}
