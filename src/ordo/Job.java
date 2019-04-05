package ordo;

import config.Project;
import exceptions.ErreurJobException;
import exceptions.AlreadyExists;
import formats.Format;
import formats.KVFormat;
import formats.LineFormat;
import formats.HdfsQuery;
import formats.HdfsResponse;
import map.MapReduce;
import hdfs.DataNode;
import hdfs.HdfsClient;
import hdfs.InfoFichier;

import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.Hashtable;
import java.lang.System;

public class Job extends UnicastRemoteObject implements JobInterface, CallBack {

	// Pour notifier le moniteur quand on a fini
	private final Collection<JobInterface> jobQ;

	// Fichier sur lequel effectuer le reduce
	private String fname;

	// nombre de map restant (non fini)
	private int chunkMapped;

	// fichier résultat des maps sur le dhfs (à reduce ensuite)
	private InfoFichier result = null;

	// résultat pour le moniteur
	public InfoJob analyse;

	Job(Collection<JobInterface> jobQ) throws RemoteException {
		this.jobQ = jobQ;
		this.analyse = new InfoJob();
	}

	@Override
	public void setInputFname(String fname) {
		this.fname = fname;
	}

	@Override
	public void startJob(MapReduce mr) throws ErreurJobException {

		// Look for information on the file in hdfs
		InfoFichier info = null;
		try {
			info = HdfsClient.HdfsList(this.fname);
		} catch (Exception e) {
			throw new ErreurJobException(e.getMessage());
		}

		this.analyse.flength = info.getTaille();

		Format reader = null, writer;
		switch (info.getFormat()) {
			case KV:
				reader = new KVFormat();
				break;
			case LINE:
				reader = new LineFormat();
				break;
		}
		writer = new KVFormat(); // Output mapped file

		// Fichier résultat crée sur le hdfs
		this.result = new InfoFichier(this.fname+".map", Format.Type.KV);
		this.analyse.fname = this.fname+".map";

		Hashtable<Integer,Inet4Address> dataNodes = info.getChunks();
		this.chunkMapped = dataNodes.size();
		this.analyse.totalMapTime = (double)System.currentTimeMillis() / 1000F;
		// Start map on DaemonDataNodes
		for (Integer i : dataNodes.keySet()) {
			Inet4Address address = dataNodes.get(i);
			try {
				// Connexion au daemonDataNode
				Daemon daemon = (Daemon) Naming.lookup("//" + address.getHostAddress() + ':' + Project.RMI_PORT + '/' + Project.RMI_DAEMON);
				System.out.println("Starting map on daemonDataNode " + address.toString());

				// Noms fichiers lecture et écriture spécifique au dataNode
				writer.setFname(DataNode.makeName(this.fname+".map", i));
				reader.setFname(DataNode.makeName(this.fname, i));

				// Lancer le map
				daemon.runMap(mr, reader, writer, this);
				// Mettre à jour le infoFichier résultat
				this.result.addChunk(i, address);
			} catch (NotBoundException e) {
				System.out.println("Error: Registry not found.");
				throw new ErreurJobException("Registry not found");
			} catch (MalformedURLException e) {
				System.out.println("Error: Host not found.");
				throw new ErreurJobException("Host " + address.toString() + " not found");
			} catch (RemoteException e) {
				throw new ErreurJobException("Erreur connexion au DaemonDataNode");
			}
		}
	}

	@Override
	public synchronized void mapDone(Integer chunk, Double tps)  {
		this.analyse.mapTimes.put(chunk, tps); // Analyse de perf
		if (--this.chunkMapped == 0) {
			this.analyse.totalMapTime = ((double)System.currentTimeMillis() / 1000F) - this.analyse.totalMapTime;
			System.out.println("Map done.");
			// Ajouter fichier résultat sur le nameNode
			HdfsQuery query = new HdfsQuery(HdfsQuery.Command.WRT_FILE, this.result);
			boolean chg_name = true;
			while(chg_name) {
				try {
					HdfsResponse response = HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);
					chg_name = false;
				} catch(AlreadyExists e) {
					String nom = this.result.getNom();
					nom = nom + "p";
				} catch(Exception e) {
					System.err.println("Fatal Error: nameNode specified isn't a nameNode");
					this.result = null;
					chg_name = false;
				}
			}
			synchronized (this.jobQ) {
				this.jobQ.notify();
			}
		}
		else System.out.println("One map done, " + this.chunkMapped + " more to go");
	}
}
