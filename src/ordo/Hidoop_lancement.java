package ordo;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Scanner;

import application.MyMapReduce;
import config.Project;
import formats.Format;
import hdfs.HdfsClient;
import hdfs.HdfsServer;
import hdfs.InfoFichier;

public class Hidoop_lancement {

	public static void lancer(String fname){
		try {
			Runtime.getRuntime().exec("java ordo.Daemon_dataNode 6060");

			InfoFichier info_f = HdfsClient.HdfsList(fname);
			Hashtable<Integer, Inet4Address> addrs =  info_f.getChunks();
			String user = System.getenv("USER");

			for (Inet4Address host : new LinkedHashSet<Inet4Address>(addrs.values()) ) {
				String host_s = host.toString().split("/")[0];
				System.out.println("ssh "+user+"@"+host_s+" cd "+Project.PATH+"src && java ordo.Daemon_dataNode 6060");
				Runtime.getRuntime().exec("ssh "+user+"@"+host_s+" cd "+Project.PATH+"src && java ordo.Daemon_dataNode 6060");

			}
			Thread.sleep(3000);

			Job j = new Job();
	        j.setInputFormat(Format.Type.LINE);
	        j.setOutputformat(Format.Type.KV);
	        j.setInputFname(fname);
	        j.setDataNode(addrs);
	        j.setReducer("yoda");
	        j.startJob(new MyMapReduce());


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void main(String[] args){
		InterfaceMapReduce.showHelp();

		Thread thread = new Thread(){
		    @Override
			public void run(){
		    	String[] ah = {"-n"};
				HdfsServer.main(ah);
		    }
		  };
		  thread.start();

		String user = System.getenv("USER");
		ArrayList<String> hosts = new ArrayList<String>();

        Scanner input = new Scanner(System.in);
        String cmd;
        boolean exit = false;

        System.out.println("Console Hidoop v0:");
        do {
            System.out.print("~$> ");
            cmd = input.nextLine();
            String[] lcmd = cmd.split(" ");
            String base = lcmd[0];
            switch (base) {
                case "e":
                case "exit":
                    exit = true;
                    break;
                case "ajouter":
					try {
						String host = lcmd[1];
	            		hosts.add(host);
						Runtime.getRuntime().exec("ssh "+user+"@"+host+" cd "+Project.PATH+"src && java hdfs.HdfsServer yoda");
					} catch (Exception e1) {
						System.out.println("Erreur ajout datanode");
					}

                	break;
                case "write":
                	try {
	                	 String[] arguw = {"write","line",lcmd[1]};
	                     HdfsClient.main(arguw);
                	} catch (Exception e){
                		String[] arguw = {"write"};
                		HdfsClient.main(arguw);
                	}
                     break;
                case "read":

                	try {
                		String[] argur = {"read",lcmd[1],lcmd[2]};
                		HdfsClient.main(argur);
                	} catch (Exception e){
                		String[] argur = {"read"};
                		HdfsClient.main(argur);
                	}

                    break;
                case "delete":
                	try {
	                	String[] argud = {"delete",lcmd[1]};
	                    HdfsClient.main(argud);
                	} catch (Exception e){
                		String[] argud = {"delete"};
                		HdfsClient.main(argud);
                	}
                    break;
                case "lancer":
                	Hidoop_lancement.lancer(lcmd[1]);
                	break;
                default:
                    System.out.println("Commande inconnue.");
                    break;
            }
        } while (!exit);
        try {
			for (String host : hosts) {
				Runtime.getRuntime().exec("ssh "+user+"@"+host+" pkill java");
			}
			Runtime.getRuntime().exec("pkill java");
		} catch (IOException e1) {
			e1.printStackTrace();
		}

        System.exit(0);
	}

}
