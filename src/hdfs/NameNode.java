package hdfs;

import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

// Pistes d'amélioration --> changer le boolean de liste_fichiers_occupés en une enumeration pour permettre
// de différencier l'occupation par écriture et lecture afin de faire de la lecture en parallèle
class NameNode {
	
	private ArrayList<InfoFichier> catalogue;
	private ArrayList<Inet4Address> liste_DataNodes;
	private Hashtable<String,Boolean> liste_fichiers_occupés;
	
	public NameNode() {
		this.catalogue = new ArrayList<InfoFichier>();
		this.liste_DataNodes = new ArrayList<Inet4Address>();
		this.liste_fichiers_occupés = new Hashtable<String,Boolean>();
	}
	
	// Permet d'ajouter un fichier au catalogue de NameNode si il n'est
	// pas déjà présent, il est immédiatement disponible.
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
	
	// Retourne true si le fichier de nom "nom" est occupé, false sinon
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
		SlaveKeepAlive keepalive = new SlaveKeepAlive(namenode);
		keepalive.start();
		
		
	}

	
	
	
}
