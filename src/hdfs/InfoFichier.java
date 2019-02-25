package hdfs;

import java.io.Serializable;
import java.net.Inet4Address;
import java.util.Hashtable;

import formats.Format;

public class InfoFichier implements Serializable {

	private Hashtable<Integer,Inet4Address> chunks; // Clef -> ChunkHandle, Valeur -> Adresse où se trouve le chunk
	private Hashtable<Integer,Inet4Address> chunksBackup; // Clef -> ChunkHandle, Valeur -> Addresse où se trouve le backup du chunk

	private String nom; // Nom du fichier
	private Format.Type format; // format kv ou line

	public InfoFichier(String nom, Format.Type format) {
		this.chunks = new Hashtable<Integer,Inet4Address>();
		this.chunksBackup = new Hashtable<Integer,Inet4Address>();
		this.nom = nom;
		this.format = format;
	}

	public void addChunk(int numero, Inet4Address addr) {
		this.chunks.put(numero, addr);
	}
	public void addBackup(int numero, Inet4Address addr){
		this.chunksBackup.put(numero, addr);
	}
	public void switchToBackup(int numero){
		this.chunks.replace(numero, this.chunksBackup.get(numero));
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
