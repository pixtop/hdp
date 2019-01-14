package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.rmi.RemoteException;
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
	int maxTries = 3;
	int count = 0;
		while(true) {

		try {
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			KV kv;
			while ((kv = (KV) ois.readObject())!=null) {
				this.resultat.add(kv);
			}
			break;

			} catch (RemoteException e) {
				System.out.println("Erreur de l'invocation à distance");
			} catch (IOException e) {
		   		if (++count == maxTries) {
					System.out.println("Ce fichier:"+fname+" n'existe pas !");
		   		}
			} catch (Exception e1) {
				System.out.println("Erreur innatendue dans envoyerVers");
			}
		}

	}
}
