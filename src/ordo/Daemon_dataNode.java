package ordo;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import map.MapReduce;

public class Daemon_dataNode extends UnicastRemoteObject implements Daemon{


	public Daemon_dataNode() throws RemoteException {

	}

	@Override
	public void runMap(MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException {
		System.out.println("RunMap en cours !");
		m.map(reader, writer);
		// TODO: utliser le callback (peut etre?)
	}

	@Override
	public void runReduce(MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException {
		System.out.println("RunReduce en cours !");
		m.reduce(reader, writer);
		// TODO: utliser le callback (peut etre?)
	}

	@Override
	public void envoyerVers(String addr,int port,String name){

		try {
			Socket s = new Socket(addr,port);
			ObjectOutputStream ois = new ObjectOutputStream(s.getOutputStream());
			KVFormat reader = new KVFormat();
			reader.setFname(name);
			KV kv;
			while ((kv = reader.read()) != null) {
				ois.writeObject(kv);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void recevoir(int nbData,int port){
		ServerSocket ss;
		try {
			ss = new ServerSocket(port);
			for(int i=0;i<nbData;i++){
				SlaveRecevoir sl = new SlaveRecevoir(ss.accept());
				sl.start();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
