package hdfs;

import java.net.Inet4Address;
import java.util.Hashtable;

public class InfoFichier {
	
	private Hashtable<Integer,Inet4Address> chunks; // Clef -> ChunkHandle, Valeur -> Adresse trouve le chunk
	private String nom; // Nom du fichier
	private int taille; // Taille du fichier
	
	
	public InfoFichier(String nom, int taille) {
		this.chunks = new Hashtable<Integer,Inet4Address>();
		this.nom = nom;
		this.setTaille(taille);
	}
	
	public void addChunk(int numero, Inet4Address addr) {
		this.chunks.put(numero, addr);
	}
	
	public String getNom() {
		return this.nom;
	}
	
	public Hashtable<Integer,Inet4Address> getChunks(){
		return this.chunks;
	}

	public int getTaille() {
		return taille;
	}

	public void setTaille(int taille) {
		this.taille = taille;
	}


}