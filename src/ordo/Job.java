package ordo;

import config.Project;
import exceptions.ErreurJobException;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import map.MapReduce;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;

public class Job extends UnicastRemoteObject implements JobInterface, CallBack {

	private final Collection<JobInterface> jobQ;
	private Collection<InetAddress> daemonList;
	private String fname;
	private Format.Type ftype;
	private int chunkMapped = 0;

	Job(Collection<JobInterface> jobQ, Collection<InetAddress> dl) throws RemoteException {
		this.jobQ = jobQ;
		this.daemonList = dl;
	}

	@Override
	public void setInputFormat(Format.Type ft) {
		this.ftype = ft;
	}

	@Override
	public void setInputFname(String fname) {
		this.fname = fname;
	}

	@Override
	public void startJob(MapReduce mr) throws ErreurJobException {
		Daemon daemon;
		Format reader = null, writer;
		if (this.ftype == null) throw new ErreurJobException();
		switch (this.ftype) {
			case KV:
				reader = new KVFormat();
				break;
			case LINE:
				reader = new LineFormat();
				break;
		}
		reader.setFname(this.fname);
		// Output mapped file
		writer = new KVFormat();
		writer.setFname(this.fname+".map");
		// Start map on DaemonDataNodes
		for (InetAddress address : daemonList) {
			try {
				daemon = (Daemon) Naming.lookup("//" + address.getHostAddress() + ':' + Project.RMI_PORT + '/' + Project.RMI_DAEMON);
				System.out.println("Starting map on " + this.daemonList.size() + " DataNodes.");
				daemon.runMap(mr, reader, writer, this);
			} catch (NotBoundException e) {
				System.out.println("Error: Registry not found.");
				throw new ErreurJobException();
			} catch (MalformedURLException e) {
				System.out.println("Error: Host not found.");
				throw new ErreurJobException();
			} catch (RemoteException e) {
				throw new ErreurJobException();
			}
		}
	}

	@Override
	public synchronized void mapDone() {
		if (++this.chunkMapped == this.daemonList.size()) {
			System.out.println("Map done.");
			synchronized (this.jobQ) {
				this.jobQ.notify();
			}
		}
		else System.out.println(this.chunkMapped + " map out of " + this.daemonList.size() + " is done.");
	}
}