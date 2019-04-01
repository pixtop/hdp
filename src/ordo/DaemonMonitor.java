package ordo;

import config.Project;
import exceptions.ErreurJobException;
import formats.Format;
import map.MapReduce;
import hdfs.HdfsClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.Collection;

/**
 * DaemonMonitor, Moniteur des jobs lancé sur le hdfs
 * Lancement : java DaemonMonitor [adresse_nameNode]
 * Port par défaut : voir dans config.Projet
 * Utiliser HidoopServer plutôt pour le lancer
*/
public class DaemonMonitor extends UnicastRemoteObject implements RessourceManager {

    private final Collection<JobInterface> jobQ;

    public DaemonMonitor(String nameNode) throws RemoteException, UnknownHostException {

        this.jobQ = new ArrayList<>();
        HdfsClient.nameNode = InetAddress.getByName(nameNode).getHostName(); // Vérifier que l'host existe
    }

    @Override
    public InfoJob doJob(MapReduce mr, String fname) throws RemoteException, ErreurJobException {
        Job job = new Job(this.jobQ);
        job.setInputFname(fname);
        synchronized (jobQ) {
            this.jobQ.add(job);
            job.startJob(mr);
            try {
              jobQ.wait();
            } catch(InterruptedException e) {
              e.printStackTrace();
              throw new ErreurJobException(e.getMessage());
            }

            // Job is just finished
            jobQ.remove(job);
        }

        return job.analyse; // Nom fichier résultat du hdfs à reduce en local + info sur le job
    }

    @Override
    public String getNameNode() throws RemoteException {
      return HdfsClient.nameNode;
    }

    // Usage : java DaemonMonitor [nameNode_address]
    public static void main(String[] args) throws RemoteException {
        DaemonMonitor dm = null;
        if(args.length > 1) {
          try {
            dm = new DaemonMonitor(args[0]);
          } catch (UnknownHostException e) {
            System.err.println("Error: host " + args[0] + " unknown");
            System.exit(1);
          }
        } else {
          try {
            dm = new DaemonMonitor("localhost");
          } catch (UnknownHostException e) {
            System.err.println("Error: host localhost unknown");
            System.exit(1);
          }
        }
        // Création serveur DaemonMonitor en local
        try {
          LocateRegistry.createRegistry(Project.RMI_PORT);
          Naming.rebind("//"+ InetAddress.getLocalHost().getHostAddress()+":"+Project.RMI_PORT+"/"+Project.RMI_MONITOR, dm);
        } catch (RemoteException e) {
          System.err.println("Error: Port " + Project.RMI_PORT + " already in used");
          System.exit(1);
        } catch (UnknownHostException | MalformedURLException e) {
          System.err.println("Error: Unknown host error (should not happen)");
          e.printStackTrace();
          System.exit(1);
        }
    }
}
