package ordo;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import application.MyMapReduce;
import formats.*;
import map.MapReduce;

public class Job implements JobInterface{
	
	private Format.Type inputformat;
	private String inputfname;
	
	public void setInputFormat(Format.Type format) {
		this.inputformat = format;
	}
	public void setInputFname(String fname) {
		this.inputfname = fname;
	}
	public void startJob (MapReduce mr) {
		ArrayList<Inet4Address> liste_addr= new ArrayList<Inet4Address>();
		int NB_NODES = 1;//liste_addr.size();
		int port = 6060;
		
		// TODO: changer la valeur de port / ajouter un call pour obtenir la liste des addresses / ajouter un vrai callback
		for (int i=0; i<NB_NODES; i++) {
			try {
			//	System.out.print("Connexion à "+"//" + liste_addr.get(i).toString()+":"+port+"/Daemon_dataNode");
			//	Daemon obj = (Daemon) Naming.lookup("//" + liste_addr.get(i).toString()+":"+port+"/Daemon_dataNode");
				Daemon obj = (Daemon) Naming.lookup("//" + "localhost:"+port+"/Daemon_dataNode");
				
				if (this.inputformat == Format.Type.LINE) {
					System.out.println("Runmap !");
					LineFormat lf = new LineFormat();
					lf.setFname(this.inputfname);
					obj.runMap(mr,lf , lf, new CallBack());
				
				} else if (this.inputformat == Format.Type.KV){
					obj.runMap(mr, new KVFormat(), new KVFormat(), new CallBack());
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

} 