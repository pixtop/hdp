package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.MapReduce;

public interface Daemon extends Remote {
	public void runMap (MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException;
	public void runReduce (MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException;
	public void envoyerVers(String addr,int port,String name) throws RemoteException;
	public void recevoir(int nbData,int port,String fname) throws RemoteException;
}
