package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// Les messages KeepAlive sont des Inet4Address (l'addresse de DataNode)
// Ces messages doivent être envoyés sur le port 8080
public class SlaveKeepAlive extends Thread{
	private NameNode master;
	public NameNode getMaster() {
		return master;
	}

	private ArrayList<Inet4Address> dataNodes;
	
	public SlaveKeepAlive(NameNode namenode) {
		master = namenode;
		dataNodes = master.getDataNodes();
	}
	
	public void run() {
		// Toutes les 5 secondes on supprime les dataNodes qui n'ont pas répondu au keepAlive
		ScheduledExecutorService scheduler =
				Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> KeepAlivePeriodique =
		    scheduler.scheduleAtFixedRate(new KeepAlive(this), 5, 5, TimeUnit.SECONDS);
		ServerSocket serveur;

		try {
			serveur = new ServerSocket(8080);
			while (true) {
				// Si on reçoi un message de keepAlive d'un node on l'enleve de la liste
				Slave sl = new Slave(serveur.accept());
				this.removeDataNode(sl.reception());
				
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    }
	
	private void removeDataNode(Inet4Address addr) {
		if (addr!=null) {
			Iterator<Inet4Address> i = this.dataNodes.iterator();
			while (i.hasNext()) {
				Inet4Address a = i.next();
				if (a.equals(addr)) {
					this.dataNodes.remove(a);
					break;
				}
			}
		}
	}

	public ArrayList<Inet4Address> getDataNodes() {
		return dataNodes;
	}

	public void setDataNodes(ArrayList<Inet4Address> dataNodes) {
		this.dataNodes = dataNodes;
	}
	
} class KeepAlive implements Runnable {
	SlaveKeepAlive slave;
	
	public KeepAlive(SlaveKeepAlive slave) {
		this.slave = slave;
	}
	
	public void run() {
		Iterator<Inet4Address> i = slave.getDataNodes().iterator();
		while (i.hasNext()) {
			// Un des dataNode n'a pas répondu -> On le supprime
			slave.getMaster().removeDataNode(i.next());
			
		}
		// On remet dataNodes à 0 et on recommence
		synchronized(slave.getDataNodes()) {
			slave.setDataNodes(slave.getMaster().getDataNodes());
		}
	}
} 
class Slave extends Thread {
	 Socket ssock;
	 
	 public Slave(Socket s) {
		 this.ssock = s;
	 }
	 public Inet4Address reception() {
		ObjectInputStream ois;
		Inet4Address addr = null;
		try {
			ois = new ObjectInputStream(ssock.getInputStream());
			addr = (Inet4Address)ois.readObject();
			ssock.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return addr;
	}
}
