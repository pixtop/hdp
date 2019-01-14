package ordo;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;

public class SlaveEnvoyerVers extends Thread{

	private int port;
	private String addr_vers;
	private String fname;
	private int port2;
private String addr;
	public SlaveEnvoyerVers(String a,String av,int p, String fname,int port2){
		port = p;
		addr = a;
		addr_vers = av;
		this.fname = fname;
		this.port2 = port2;
	}
	@Override
	public void run(){

		Daemon obj;
		try {
			obj = (Daemon) Naming.lookup("//" + addr+":"+port+"/Daemon_dataNode");
			obj.envoyerVers(addr_vers,port2,fname);

		} catch (RemoteException e) {
			System.out.println("Erreur de l'invocation à distance");
		} catch (IOException e) {
			System.out.println("Ce fichier n'existe pas !");
		} catch (Exception e1) {
			System.out.println("Erreur innatendue dans envoyerVers");
		}

	}
}
