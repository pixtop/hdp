package ordo;

import java.net.Inet4Address;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Hashtable;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class Job implements JobInterface{

	private Format.Type inputformat;
	private Format.Type outputformat;
	private String inputfname;
	private String outputfname;
	private String outputfnameReduce;

	private Hashtable<Integer, Inet4Address> liste_addr= new Hashtable<Integer, Inet4Address>();
	private String addr_reduce;

	private final int port_remote = 6060; // port pour remote
	private final int port_data_transfer = 6660; // port pour transfert de données entre les datanodes (pour faire le reduce)


	public String getInputfname() {
		return inputfname;
	}
	@Override
	public void setInputFname(String fname) {
		this.inputfname = fname;
	}

	public String getOutputfname() {
		return outputfname;
	}
	public void setOutputFname(String fname) {
		this.outputfname = fname;
	}


	public Format.Type getInputformat() {
		return inputformat;
	}
	@Override
	public void setInputFormat(Format.Type format) {
		this.inputformat = format;
	}
	public Format.Type getOutputformat() {
		return outputformat;
	}
	public void setOutputformat(Format.Type outputformat) {
		this.outputformat = outputformat;
	}


	public void setDataNode(Hashtable<Integer, Inet4Address> t){
		liste_addr = t;
	}
	public void setReducer(String hostname){
		addr_reduce = hostname;
	}

	@Override
	public void startJob (MapReduce mr) {

		int NB_NODES = liste_addr.size();
		SlaveMap[] slaves = new SlaveMap[NB_NODES];

		this.outputfname = this.inputfname+"-map";
		this.outputfnameReduce = this.inputfname+"-red";



		int ii = 0;
		// TODO: changer la valeur de port / ajouter un call pour obtenir la liste des addresses / ajouter un vrai callback
		for (Integer i: liste_addr.keySet()) {
			try {
			//	Daemon obj = (Daemon) Naming.lookup("//" + "localhost:"+port+"/Daemon_dataNode");
				slaves[ii] = new SlaveMap(liste_addr.get(i).toString().split("/")[0],port_remote,this,mr,new CallBack(),this.inputfname+"."+i,this.outputfname+"."+i);
				slaves[ii].start();
				ii++;
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
		System.out.println("------ FIN MAP ------");

		System.out.println("Debut de reception");

		// On obtient l'addresse du datanode qui fait le reduce
		int i_reduce = 0;
		Daemon obj;
		Thread thread = new Thread(){
		    @Override
			public void run(){
		    	try {
					Daemon obj = (Daemon) Naming.lookup("//" + addr_reduce +":"+port_remote+"/Daemon_dataNode");
					obj.recevoir(NB_NODES, port_data_transfer, outputfnameReduce);
				} catch (MalformedURLException | RemoteException | NotBoundException e1) {
					e1.printStackTrace();
				}
		    }
		  };
		  thread.start();



		ii = 0;
		SlaveEnvoyerVers[] slaves_e = new SlaveEnvoyerVers[NB_NODES];
		// Il faut tout envoyer vers le reducer
		for (Integer i: liste_addr.keySet()) {
			if (liste_addr.get(i).toString().split("/")[0] != addr_reduce) {
				try {
					slaves_e[ii] = new SlaveEnvoyerVers(liste_addr.get(i).toString().split("/")[0],addr_reduce,port_remote,this.outputfname+"."+i,port_data_transfer);
					slaves_e[ii].start();
					ii++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				i_reduce = i;
			}
		}

		for (int i=0; i<NB_NODES; i++) {
			if (i!= i_reduce) {
				try {
					slaves_e[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

		}
		try {
			thread.join();
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}

		System.out.println("Fin de reception");
		System.out.println("Debut de Reduce");

		try {
			obj = (Daemon) Naming.lookup("//" + addr_reduce +":"+port_remote+"/Daemon_dataNode");
			System.out.println("Connecté à "+"//" + addr_reduce+":"+port_remote+"/Daemon_dataNode"+" pour reduce");
			Format reader_reduce=null;
			Format writer_reduce=null;
			if (this.outputformat == Format.Type.LINE) {
				reader_reduce = new LineFormat();
				writer_reduce = new LineFormat();
			} else if (this.outputformat == Format.Type.KV){
				reader_reduce = new KVFormat();
				writer_reduce = new KVFormat();
			}
			reader_reduce.setFname(this.outputfnameReduce);
			writer_reduce.setFname(this.outputfnameReduce+"fin");
			obj.runReduce(mr, reader_reduce, writer_reduce, new CallBack());
		} catch (MalformedURLException | RemoteException | NotBoundException e) {
			e.printStackTrace();
		}

		System.out.println("------ FIN REDUCE ------");


	}

}