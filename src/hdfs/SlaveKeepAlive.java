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
// Ces messages doivent etre envoyes sur le port 8080
public class SlaveKeepAlive extends Thread{
	private NameNode master;
	private ArrayList<Inet4Address> dataNodes;
	
	public SlaveKeepAlive(NameNode namenode) {
		master = namenode;
		dataNodes = master.getDataNodes();
	}
	
	public void run() {
		// Toutes les 5 secondes on supprime les dataNodes qui n'ont pas repondu au keepAlive
		ScheduledExecutorService scheduler =
				Executors.newScheduledThreadPool(1);
		ScheduledFuture<?> KeepAlivePeriodique =
		    scheduler.scheduleAtFixedRate(new KeepAlive(this), 5, 5, TimeUnit.SECONDS);
		ServerSocket serveur;

		try {
			serveur = new ServerSocket(8080);
			while (true) {
				// Si on reenvoi un message de keepAlive d'un node on l'enleve de la liste
				Slave sl = new Slave(serveur.accept(),this);
				sl.start();			
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
		
    }
	
	protected void removeDataNode(Inet4Address addr) {
		synchronized(this.dataNodes) {
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
	}

	protected ArrayList<Inet4Address> getDataNodes() {
		synchronized(this.dataNodes) {
			return dataNodes;
		}
	}

	protected void setDataNodes(ArrayList<Inet4Address> dataNodes) {
		synchronized(this.dataNodes) {
			this.dataNodes = dataNodes;
		}
	}
	
	protected NameNode getMaster() {
		return master;
	}
	
} 

class KeepAlive implements Runnable {
	SlaveKeepAlive slave;
	
	public KeepAlive(SlaveKeepAlive slave) {
		this.slave = slave;
	}
	
	public void run() {
		Iterator<Inet4Address> i = slave.getDataNodes().iterator();
		while (i.hasNext()) {
			// Un des dataNode n'a pas repondu -> On le supprime
			slave.getMaster().removeDataNode(i.next());
			
		}
		// On remet dataNodes a 0 et on recommence
		slave.setDataNodes(slave.getMaster().getDataNodes());

	}
} 

class Slave extends Thread {
	 private Socket ssock;
	 private SlaveKeepAlive slave;
	 
	 public Slave(Socket s,SlaveKeepAlive slave) {
		 this.ssock = s;
	 }
	 
	 public void run() {
		ObjectInputStream ois;
		try {
			ois = new ObjectInputStream(ssock.getInputStream());
			Inet4Address addr = (Inet4Address)ois.readObject();
			ssock.close();
			if (!slave.getMaster().estPresente(addr)) {
				// Si c'est une nouvelle adresse on l'ajoute
				slave.getMaster().addDataNode(addr);
			} else {
				// Sinon on la supprime de la liste des DataNodes qui n'ont pas 
				// repondu
				slave.removeDataNode(addr);	
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }	 

}
