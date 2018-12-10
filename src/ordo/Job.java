package ordo;

import java.net.Inet4Address;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.ArrayList;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class Job implements JobInterface{

	private Format.Type inputformat;
	private Format.Type outputformat;
	private String inputfname;

	public Format.Type getInputformat() {
		return inputformat;
	}

	public String getInputfname() {
		return inputfname;
	}

	@Override
	public void setInputFormat(Format.Type format) {
		this.inputformat = format;
	}
	@Override
	public void setInputFname(String fname) {
		this.inputfname = fname;
	}
	@Override
	public void startJob (MapReduce mr) {
		ArrayList<Inet4Address> liste_addr= new ArrayList<Inet4Address>();
		int NB_NODES = liste_addr.size();
		int port = 6060;
		SlaveMap[] slaves = new SlaveMap[NB_NODES];

		// TODO: changer la valeur de port / ajouter un call pour obtenir la liste des addresses / ajouter un vrai callback
		for (int i=0; i<NB_NODES; i++) {
			try {
			//	Daemon obj = (Daemon) Naming.lookup("//" + "localhost:"+port+"/Daemon_dataNode");
				slaves[i] = new SlaveMap(liste_addr.get(i).toString(),port,this,mr,new CallBack());
				slaves[i].start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		// On attend la fin de l'execution des maps, on utilise pas callback
		for (int i=0; i<NB_NODES; i++) {
			try {
				slaves[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// On obtient l'addresse du datanode qui fait le reduce
				String addr_reduce = "";

		// Il faut tout envoyer vers le reducer
		for (int i=0; i<NB_NODES; i++) {
			if (liste_addr.get(i).toString() != addr_reduce) {
				try {
					//	Daemon obj = (Daemon) Naming.lookup("//" + "localhost:"+port+"/Daemon_dataNode");
					slaves[i] = new SlaveEnvoyerVers(liste_addr.get(i).toString(),port,this,mr,new CallBack());
					slaves[i].start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}


		Daemon obj;
		try {
			obj = (Daemon) Naming.lookup("//" + addr_reduce +":"+port+"/Daemon_dataNode");
			System.out.print("Connecté à "+"//" + addr_reduce+":"+port+"/Daemon_dataNode"+" pour reduce");
			Format t=null;
			if (this.outputformat == Format.Type.LINE) {
				t = new LineFormat();
			} else if (this.outputformat == Format.Type.KV){
				t = new KVFormat();
			}
			obj.runReduce(mr, t, t, new CallBack());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}


	}
	public Format.Type getOutputformat() {
		return outputformat;
	}
	public void setOutputformat(Format.Type outputformat) {
		this.outputformat = outputformat;
	}

}