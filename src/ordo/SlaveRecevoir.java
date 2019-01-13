package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.ArrayList;

import formats.KV;

public class SlaveRecevoir extends Thread{

	private Socket s;
	private String fname;
	private ArrayList<KV> resultat;

	public SlaveRecevoir(Socket s, String fn){
		this.s = s;
		fname = fn;
		this.resultat = new ArrayList<KV>();
	}

	public ArrayList<KV> getResultat(){
		return this.resultat;
	}

	@Override
	public void run(){

		try {
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			KV kv;
			while ((kv = (KV) ois.readObject())!=null) {
				this.resultat.add(kv);
			}

		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
