package ordo;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Scanner;

import application.MyMapReduce;
import config.Project;
import formats.Format;
import hdfs.HdfsClient;
import hdfs.HdfsServer;
import hdfs.InfoFichier;

public class Hidoop_lancement {

	public static void help(){
		 System.out.println("		HELP:");
		 System.out.println("	lancer $fname  lance MapReduce sur la fichier $fname");
         System.out.println("	ajouter $hostname: lance datanode sur la machine $hostname");
         System.out.println("	write $fname: met le fichier de nom $fname dans le système hdfs");
         System.out.println("	delete $fname: supprime le fichier de nom $fname du système hdfs");
         System.out.println("	read $fname: recupère le fichier de nom $fname du système hdfs");
	}

	public static void lancer(String fname,String ADDRLOCAL){
		try {
			long t1 = System.currentTimeMillis();

			InfoFichier info_f = HdfsClient.HdfsList(fname);
			Hashtable<Integer, Inet4Address> addrs =  info_f.getChunks();

			Job j = new Job();
	        j.setInputFormat(Format.Type.LINE);
	        j.setOutputformat(Format.Type.KV);
	        j.setInputFname(fname);
	        j.setDataNode(addrs);
	        j.setReducer(ADDRLOCAL);
	        j.startJob(new MyMapReduce());

	        long t2 = System.currentTimeMillis();
	        System.out.println("time in ms ="+(t2-t1));
		} catch (Exception e) {
			System.out.println("Erreur, lancement annulé");
		}

	}
	public static void main(String[] args){
		Thread thread = new Thread(){
		    @Override
			public void run(){
		    	String[] ah = {"-n"};
				HdfsServer.main(ah);
		    }
		  };
		thread.start();
		String ADDRLOCAL = "";

		try {
			Runtime.getRuntime().exec("java ordo.Daemon_dataNode 6060");
			ADDRLOCAL = InetAddress.getLocalHost().getHostName();
		} catch (Exception e) {
			System.out.println("Erreur critique, vérifiez que aucun programme n'utilise le port 6060.");
			System.exit(1);
		}


		String user = System.getenv("USER");
		ArrayList<String> hosts = new ArrayList<String>();

        Scanner input = new Scanner(System.in);
        String cmd;
        boolean exit = false;
        System.out.println("Console Hidoop v0:");
        Hidoop_lancement.help();
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
						Runtime.getRuntime().exec("ssh "+user+"@"+host+" cd "+Project.PATH+"src && java hdfs.HdfsServer "+ADDRLOCAL);
						Runtime.getRuntime().exec("ssh "+user+"@"+host+" cd "+Project.PATH+"src && java ordo.Daemon_dataNode 6060");
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
                	Hidoop_lancement.lancer(lcmd[1],ADDRLOCAL);
                	break;
                default:
                    System.out.println("Commande inconnue.");
                    Hidoop_lancement.help();
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
