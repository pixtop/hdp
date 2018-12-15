package hdfs;

import java.net.Inet4Address;
import java.util.Hashtable;
import java.io.Serializable;

public class InfoFichier implements Serializable {

	private Hashtable<Integer,Inet4Address> chunks; // Clef -> ChunkHandle, Valeur -> Adresse trouve le chunk
	private String nom; // Nom du fichier


	public InfoFichier(String nom) {
		this.chunks = new Hashtable<Integer,Inet4Address>();
		this.nom = nom;
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

}
