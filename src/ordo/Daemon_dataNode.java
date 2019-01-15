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
import java.util.ArrayList;

import formats.Format;
import formats.Format.OpenMode;
import formats.KV;
import formats.KVFormat;
import map.MapReduce;

public class Daemon_dataNode extends UnicastRemoteObject implements Daemon{


	/**
	 *
	 */
	private static final long serialVersionUID = 1518235174567752969L;

	public Daemon_dataNode() throws RemoteException {}

	@Override
	public void runMap(MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException,IOException {
		System.out.println("RunMap en cours !");
		reader.open(OpenMode.R);
		writer.open(OpenMode.W);
		m.map(reader, writer);
		reader.close();
		writer.close();
	}

	@Override
	public void runReduce(MapReduce m, Format reader, Format writer, CallBack cb) throws RemoteException,IOException {
		System.out.println("RunReduce en cours !");
		reader.open(OpenMode.R);
		writer.open(OpenMode.W);
		m.reduce(reader, writer);
		reader.close();
		writer.close();
	}

	@Override
	public void envoyerVers(String addr,int port,String name) throws RemoteException,IOException{


		Socket s = new Socket(addr,port);
		ObjectOutputStream ois = new ObjectOutputStream(s.getOutputStream());
		KVFormat reader = new KVFormat();
		reader.setFname(name);
		KV kv;
		reader.open(OpenMode.R);
		while ((kv = reader.read()) != null) {
			ois.writeObject(kv);
		}
		reader.close();
		ois.writeObject(null);
		s.close();


	}

	@Override
	public void recevoir(int nbData,int port,String fname) throws RemoteException,IOException,InterruptedException{
		ServerSocket ss;
		ArrayList<SlaveRecevoir> sl = new ArrayList<SlaveRecevoir>();

		ss = new ServerSocket(port);
		for(int i=0;i<nbData;i++){
			sl.add(new SlaveRecevoir(ss.accept(),fname));
			sl.get(i).start();
		}


		ArrayList<KV> resultat_recep = new ArrayList<KV>();
		for (int i=0; i<nbData; i++) {

				sl.get(i).join();
				resultat_recep.addAll(sl.get(i).getResultat());


		}

		KVFormat writer = new KVFormat();
		writer.setFname(fname);

		writer.open(OpenMode.W);
		for (KV kv : resultat_recep) {
			writer.write(kv);
		}
		writer.close();


		ss.close();

		System.out.println("Reception terminée !");

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
			System.out.println("Erreur critique de lancement du Daemon");
		}
	}

}
