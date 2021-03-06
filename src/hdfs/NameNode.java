package hdfs;

import java.io.Serializable;
import java.net.Inet4Address;
import java.util.*;
import exceptions.*;

// Pistes d'amelioration --> changer le boolean de liste_fichiers_occupes en une enumeration pour permettre
// de differencier l'occupation par ecriture et lecture afin de faire de la lecture en parallele
class NameNode implements Serializable {

	private Map<String, InfoFichier> catalogue;
	private ArrayList<Inet4Address> liste_DataNodes;
	private Hashtable<String,Boolean> liste_fichiers_occupes;

	public NameNode() {
		this.catalogue = new HashMap<String, InfoFichier>();
		this.liste_DataNodes = new ArrayList<Inet4Address>();
		this.liste_fichiers_occupes = new Hashtable<String,Boolean>();
	}

	// Permet d'ajouter un fichier au catalogue de NameNode si il n'est
	// pas deja present, il est immediatement disponible.
	public void ajouterFichier(InfoFichier fichier) throws AlreadyExists {
		if (this.catalogue.get(fichier.getNom()) == null) {
			this.catalogue.put(fichier.getNom(), fichier);
			this.liste_fichiers_occupes.put(fichier.getNom(), false);
		} else throw new AlreadyExists("File " + fichier.getNom() + " already exists");
	}

	// Permet d'obtenir les informations du fichier de nom "nom"
		// du catalogue de NameNode
	public InfoFichier getInfoFichier(String nom) {
		return this.catalogue.get(nom);
	}

	int getNbFiles() {
		return catalogue.size();
	}

	String[] getAllFileNames() {
		return catalogue.values().stream().map(InfoFichier::getNom).toArray(String[]::new); // Résultat = tableau de String
	}

	public void supprimerFichier(String nom) {
		this.catalogue.remove(nom);
	}

	// Retourne true si le fichier de nom "nom" est occupe, false sinon
	public boolean est_occupe(String nom) {
		return liste_fichiers_occupes.get(nom);
	}

	// Permet de donner le status du fichier de nom "nom"
	public void setStatus(String nom, Boolean status) {
		this.liste_fichiers_occupes.replace(nom, status);
	}

	public void addDataNode(Inet4Address addr) {
		this.liste_DataNodes.add(addr);
	}

	public void removeDataNode(Inet4Address addr) {
		this.liste_DataNodes.remove(addr);
	}

	public boolean estPresente(Inet4Address addr) {
		return this.liste_DataNodes.contains(addr);
	}

	public ArrayList<Inet4Address> getDataNodes() {
		return this.liste_DataNodes;
	}

}
