package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// Les messages KeepAlive sont des Inet4Address (l'addresse de DataNode)
public class SlaveKeepAlive extends Thread{

	// Ces messages doivent etre envoyes sur le port 8080
	public static int port = 8080;
	public static int delay = 10; // Délai entre les réveils du KeepAlive (en secondes)

	private NameNode master;
	private ArrayList<Inet4Address> dataNodes;

	/**
	 * @param namenode ip address of NameNode
	 */
	SlaveKeepAlive(NameNode namenode) {
		this.master = namenode;
		this.dataNodes = new ArrayList<>(this.master.getDataNodes());
	}

	@Override
	public void run() {
		// Toutes les 5 secondes on supprime les dataNodes qui n'ont pas repondu au keepAlive
		ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
		scheduler.scheduleAtFixedRate(new KeepAlive(this), SlaveKeepAlive.delay, SlaveKeepAlive.delay, TimeUnit.SECONDS);
		ServerSocket serveur;

		try {
			serveur = new ServerSocket(SlaveKeepAlive.port);
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

		@Override
		public void run() {
			for (Inet4Address inet4Address : slave.getDataNodes()) {
				// Un des dataNode n'a pas repondu -> On le supprime
				slave.getMaster().removeDataNode(inet4Address);
				System.out.println("\033[31mDataNode " + inet4Address.toString() + " down, removed from dataNodes list\033[0m");
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

		@Override
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
					System.out.println("\033[32mNew DataNode identified : " + addr.toString() + "\033[0m");
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
