package hdfs;

import java.io.Serializable;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

// Pistes d'amelioration --> changer le boolean de liste_fichiers_occupes en une enumeration pour permettre
// de differencier l'occupation par ecriture et lecture afin de faire de la lecture en parallele
class NameNode implements Serializable {
	
	private ArrayList<InfoFichier> catalogue;
	private ArrayList<Inet4Address> liste_DataNodes;
	private Hashtable<String,Boolean> liste_fichiers_occupés;
	
	public NameNode() {
		this.catalogue = new ArrayList<InfoFichier>();
		this.liste_DataNodes = new ArrayList<Inet4Address>();
		this.liste_fichiers_occupés = new Hashtable<String,Boolean>();
	}
	
	// Permet d'ajouter un fichier au catalogue de NameNode si il n'est
	// pas deja present, il est immediatement disponible.
	public void ajouterFichier(InfoFichier fichier) {
		if (!this.liste_fichiers_occupés.containsKey(fichier.getNom())) {
			this.catalogue.add(fichier);
			this.liste_fichiers_occupés.put(fichier.getNom(), false);
		}
		
	}
	
	// Permet d'obtenir les informations du fichier de nom "nom" 
		// du catalogue de NameNode
	public InfoFichier getInfoFichier(String nom) {
		Iterator<InfoFichier> i = catalogue.iterator();
		while (i.hasNext()) {
			InfoFichier f = i.next();
			if (f.getNom() == nom) {
				return f;
			}
		}
		return null;
	}
	
	// Retourne true si le fichier de nom "nom" est occupe, false sinon
	public boolean est_occupé(String nom) {
		return liste_fichiers_occupés.get(nom);
	}

	// Permet de donner le status du fichier de nom "nom"
	public void setStatus(String nom, Boolean status) {
		this.liste_fichiers_occupés.replace(nom, status);
	}
	
	
	public void addDataNode(Inet4Address addr) {
		this.liste_DataNodes.add(addr);
	}
	
	public void removeDataNode(Inet4Address addr) {
		Iterator<Inet4Address> i = this.liste_DataNodes.iterator();
		while (i.hasNext()) {
			Inet4Address a = i.next();
			if (a.equals(addr)) {
				this.liste_DataNodes.remove(a);
				break;
			}
		}
	}
	
	public boolean estPresente(Inet4Address addr) {
		Iterator<Inet4Address> i = this.liste_DataNodes.iterator();
		while (i.hasNext()) {
			Inet4Address a = i.next();
			if (a.equals(addr)) {
				return true;
			}
		}
		return false;
	}
	
	public ArrayList<Inet4Address> getDataNodes() {
		return this.liste_DataNodes;
	}
	
	public static void main(String[] args) {
		NameNode namenode = new NameNode();
		// SlaveKeepAlive ajoute/supprime les DataNodes de la liste des DataNodes de NameNode
		SlaveKeepAlive keepalive = new SlaveKeepAlive(namenode);
		keepalive.start();
		
		
	}

	
	
	
}
