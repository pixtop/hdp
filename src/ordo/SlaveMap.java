package ordo;

import java.rmi.Naming;

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

		Daemon obj;
		try {
			obj = (Daemon) Naming.lookup("//" + addr+":"+port+"/Daemon_dataNode");
			System.out.print("Connecté à "+"//" + addr+":"+port+"/Daemon_dataNode"+ " pour map");

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

		} catch (Exception e){
			e.printStackTrace();
		}

	}
}
