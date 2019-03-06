package ordo;

import application.MyMapReduce;
import exceptions.ErreurJobException;
import formats.Format;
import hdfs.RessourceManager;
import map.MapReduce;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collection;

public class DaemonMonitor extends UnicastRemoteObject implements RessourceManager {

    private final Collection<JobInterface> jobQ;

    private DaemonMonitor() throws RemoteException {
        this.jobQ = new ArrayList<>();
    }

    @Override
    public void doJob(MapReduce mr, String fname, Format.Type ft) throws RemoteException, ErreurJobException {
        Job job;
        try {
            ArrayList<InetAddress> daemonList = new ArrayList<>();
            //TODO: Use a method which return a list of DataNode IP from the NameNode + create a mapped meta-file in it
            daemonList.add(InetAddress.getLocalHost());
            job = new Job(this.jobQ, daemonList);
            job.setInputFname(fname);
            job.setInputFormat(ft);
            synchronized (jobQ) {
                this.jobQ.add(job);
                job.startJob(mr);
                jobQ.wait();
            }
            // Job is just finished
            synchronized (jobQ) {
                jobQ.remove(job);
            }
            System.out.println("Reducing..");
            //TODO: Implement reducing method (fetching mapped meta-file in NameNode)
        } catch (UnknownHostException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws RemoteException, ErreurJobException {
        DaemonMonitor dm = new DaemonMonitor();
        dm.doJob(new MyMapReduce(), args[0], Format.Type.LINE);
    }
}
