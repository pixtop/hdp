package hdfs;

import exceptions.ErreurJobException;
import formats.Format;
import map.MapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RessourceManager extends Remote {

    void doJob(MapReduce mr, String fname, Format.Type ft) throws RemoteException, ErreurJobException;
}
