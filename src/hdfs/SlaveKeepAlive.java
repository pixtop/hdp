package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.*;

// Les messages KeepAlive sont des Inet4Address (l'addresse de DataNode)
// Ces messages doivent etre envoyes sur le port 8080
public class SlaveKeepAlive extends Thread{
	private NameNode master;
	private ArrayList<Inet4Address> dataNodes;

	/**
	 * @param namenode ip address of NameNode
	 */
	SlaveKeepAlive(NameNode namenode) {
		this.master = namenode;
		this.dataNodes = new ArrayList<>(this.master.getDataNodes());
	}

	public void run() {
		// Toutes les 5 secondes on supprime les dataNodes qui n'ont pas repondu au keepAlive
		ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
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

	private void removeDataNode(Inet4Address addr) {
		synchronized(this.dataNodes) {
			this.dataNodes.remove(addr);
		}
	}

	private ArrayList<Inet4Address> getDataNodes() {
		synchronized(this.dataNodes) {
			return dataNodes;
		}
	}

	private void setDataNodes(ArrayList<Inet4Address> dataNodes) {
		synchronized(this.dataNodes) {
			this.dataNodes = dataNodes;
		}
	}

	private NameNode getMaster() {
		return master;
	}

	class KeepAlive implements Runnable {
		SlaveKeepAlive slave;

		KeepAlive(SlaveKeepAlive slave) {
			this.slave = slave;
		}

		public void run() {
			for (Inet4Address inet4Address : slave.getDataNodes()) {
				// Un des dataNode n'a pas repondu -> On le supprime
				slave.getMaster().removeDataNode(inet4Address);
			}
			// On remet dataNodes a 0 et on recommence
			ArrayList<Inet4Address> dataNodes = new ArrayList<>(slave.getMaster().getDataNodes());
			slave.setDataNodes(dataNodes);

		}
	}

	class Slave extends Thread {
		private Socket ssock;
		private SlaveKeepAlive slave;

		Slave(Socket s,SlaveKeepAlive slave) {
			this.ssock = s;
			this.slave = slave;
		}

		public void run() {
			ObjectInputStream ois;
			try {
				ois = new ObjectInputStream(ssock.getInputStream());
				Inet4Address addr = (Inet4Address)ois.readObject();
				ois.close();
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
}
