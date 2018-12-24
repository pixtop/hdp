package hdfs;

import java.net.Inet4Address;
import java.util.Hashtable;
import java.io.Serializable;
import formats.Format;

public class InfoFichier implements Serializable {

	private Hashtable<Integer,Inet4Address> chunks; // Clef -> ChunkHandle, Valeur -> Adresse trouve le chunk
	private String nom; // Nom du fichier
	private Format.Type format; // format kv ou line

	public InfoFichier(String nom, Format.Type format) {
		this.chunks = new Hashtable<Integer,Inet4Address>();
		this.nom = nom;
		this.format = format;
	}

	public void addChunk(int numero, Inet4Address addr) {
		this.chunks.put(numero, addr);
	}

	public String getNom() {
		return this.nom;
	}

	public Format.Type getFormat() {
		return this.format;
	}

	public Hashtable<Integer,Inet4Address> getChunks(){
		return this.chunks;
	}

}
