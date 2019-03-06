package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.MapReduce;

public interface Daemon extends Remote {
	void runMap (MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException;
}
