package hdfs;

import ordo.JobInterface;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RessourceManager extends Remote {

    void startJob(JobInterface job) throws RemoteException;
}
