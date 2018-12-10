package ordo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import formats.KV;
import formats.KVFormat;

public class SlaveRecevoir extends Thread{

	private Socket s;
	private String fname;

	public SlaveRecevoir(Socket s, String fn){
		this.s = s;
		fname = fn;
	}
	@Override
	public void run(){

		try {
			ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
			KV kv;
			KVFormat reader = new KVFormat();
			reader.setFname(fname);
			while ((kv = (KV) ois.readObject())!=null) {
				reader.write(kv);
			}

		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
