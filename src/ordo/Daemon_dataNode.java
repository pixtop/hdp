package ordo;

import java.net.InetAddress;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import map.MapReduce;
import map.Mapper;

public class Daemon_dataNode extends UnicastRemoteObject implements Daemon{


	public Daemon_dataNode() throws RemoteException {
		
	}

	@Override
	public void runMap(MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException {
		System.out.println("RunMap recu !");
		m.map(reader, writer);
		// TODO: utliser le callback
		cb.run();
	}
	
	public static void main(String args[]) {
		int port;
		try {
			Integer I= Integer.parseInt(args[0]);
			port = I.intValue();
		} catch (Exception e) {
			System.out.println("Veuillez entrer: java Daemon_dataNode <port>");
			return;
		}
		
		try {
			Registry registry = LocateRegistry.createRegistry(port);
			Daemon obj = new Daemon_dataNode();
			String URL = "//" + InetAddress.getLocalHost().getHostAddress()+":"+port+"/Daemon_dataNode";
			Naming.rebind(URL,obj);
			System.out.println("Bound in registry.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
