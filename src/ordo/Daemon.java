package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import map.MapReduce;
import map.Mapper;
import formats.Format;

public interface Daemon extends Remote {
	public void runMap (MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException;
}
