package ordo;

import hdfs.RessourceManager;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class DaemonMonitor extends UnicastRemoteObject implements RessourceManager {

    protected DaemonMonitor() throws RemoteException {}

    @Override
    public void startJob(JobInterface job) throws RemoteException {
        job.
    }

    public static void main(String[] args) {

    }
}
