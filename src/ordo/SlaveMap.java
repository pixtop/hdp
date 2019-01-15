package ordo;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;

import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

public class SlaveMap extends Thread{

	private String addr;
	private int port;
	private Job job;
	private MapReduce mr;
	private CallBack cb;
	private String inputName;
	private String outputName;

	public SlaveMap(String addr, int port, Job job, MapReduce mr,CallBack cb,String inputname,String outputname){
		this.addr = addr;
		this.port = port;
		this.job = job;
		this.mr = mr;
		this.cb = cb;
		this.inputName = inputname;
		this.outputName = outputname;
	}
	@Override
	public void run(){

		int maxTries = 3;
		int count = 0;
		Daemon obj;
		while(true) {

			try {

			obj = (Daemon) Naming.lookup("//" + addr+":"+port+"/Daemon_dataNode");
			
			Format reader_map=null;
			Format writer_map=null;
			if (job.getInputformat() == Format.Type.LINE) {
				reader_map = new LineFormat();
			} else if (job.getInputformat() == Format.Type.KV){
				reader_map = new KVFormat();
			}
			reader_map.setFname(this.inputName);

			if (job.getOutputformat() == Format.Type.LINE) {
				writer_map = new LineFormat();
			} else if (job.getOutputformat() == Format.Type.KV){
				writer_map = new KVFormat();
			}
			writer_map.setFname(this.outputName);
			obj.runMap(mr,reader_map ,writer_map , cb);
			break;

			} catch (RemoteException e) {
				System.out.println("Erreur de l'invocation à distance");
			} catch (IOException e) {
		   		if (++count == maxTries) {
					System.out.println("Ce fichier:"+inputName+" ou "+ outputName+" n'existe pas !");
		   		}
			} catch (Exception e1) {
				System.out.println("Erreur innatendue dans envoyerVers");
			}
		}



	}
}
