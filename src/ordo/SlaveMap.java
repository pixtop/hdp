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

	public SlaveMap(String addr, int port, Job job, MapReduce mr,CallBack cb){
		this.addr = addr;
		this.port = port;
		this.job = job;
		this.mr = mr;
		this.cb = cb;
	}
	@Override
	public void run(){

		Daemon obj;
		try {
			obj = (Daemon) Naming.lookup("//" + addr+":"+port+"/Daemon_dataNode");
			System.out.print("Connecté à "+"//" + addr+":"+port+"/Daemon_dataNode"+ " pour map");

			Format ti=null;
			Format to=null;
			if (job.getInputformat() == Format.Type.LINE) {
				ti = new LineFormat();
			} else if (job.getInputformat() == Format.Type.KV){
				ti = new KVFormat();
			}
			ti.setFname(job.getInputfname());

			if (job.getOutputformat() == Format.Type.LINE) {
				to = new LineFormat();


			} else if (job.getOutputformat() == Format.Type.KV){
				to = new KVFormat();
			}
			to.setFname(job.getInputfname());

			obj.runMap(mr,ti ,to , cb);

		} catch (Exception e){
			e.printStackTrace();
		}

	}
}
