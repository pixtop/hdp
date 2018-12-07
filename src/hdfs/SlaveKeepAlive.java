package hdfs;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Iterator;

public class SlaveKeepAlive extends Thread{
	private NameNode master;
	private long time_created;
	
	public SlaveKeepAlive(NameNode namenode) {
		master = namenode;
		time_created = System.currentTimeMillis();
		try {
			sleep(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void run() {
		ArrayList<Inet4Address> dataNodes = master.getDataNodes();
		while (true) {
			// Si on reçoi un message de keepAlive d'un node on l'enleve de la liste
			
			
			
			// PLUTOT FAIRE UN AUTO RUN AVEC LE TEMPS COMME POUR LE PROJET EC34x
			// Toutes les 1 secondes on supprime les dataNodes qui n'ont pas répondu au keepAlive
			if ( (System.currentTimeMillis() - time_created)%1000==0) {
				Iterator<Inet4Address> i = dataNodes.iterator();
				while (i.hasNext()) {
					// Un des dataNode n'a pas répondu -> On le supprime
					master.removeDataNode(i.next());
				}
				// On remet dataNodes à 0 et on recommence
				dataNodes = master.getDataNodes();
			}
		}	
    }
}
