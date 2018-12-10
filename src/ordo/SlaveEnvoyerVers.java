package ordo;

import java.rmi.Naming;

public class SlaveEnvoyerVers extends Thread{

	private int port;
	private String addr_vers;
	private String fname;
	private int port2;
private String addr;
	public SlaveEnvoyerVers(String a,String av,int p, String fname,int port2){
		port = p;
		addr = a;
		addr_vers = av;
		this.fname = fname;
		this.port2 = port2;
	}
	@Override
	public void run(){

		Daemon obj;
		try {
			obj = (Daemon) Naming.lookup("//" + addr+":"+port+"/Daemon_dataNode");
			obj.envoyerVers(addr_vers,port2,fname);

		} catch (Exception e){
			e.printStackTrace();
		}

	}
}
