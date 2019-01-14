package ordo;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

import application.MyMapReduce;
import config.Project;
import formats.Format;

public class InterfaceMapReduce {

	public static void showHelp(){
		System.out.print(
				"help, h : Affiche l'aide \n" +
                "exit, e : Quitte l'invité de commandes \n" +
                "data $HOSTNAME $, d $HOSTNAME : Ajoute un dataNode sur $HOSTNAME\n" +
                "lancer $FILENAME, l $FILENAME : Lance un mapReduce sur la fichier d'entrée de nom $FILENAME \n" +
                "data_rm $HOSTNAME : Supprime un dataNode sur $HOSTNAME \n"
        );
	}

	public static void main(String[] args){
		ArrayList<String> list_dataNode = new ArrayList<String>();
		InterfaceMapReduce.showHelp();
		String user = System.getenv("USER");
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setOutputformat(Format.Type.KV);


    	String local;
		try {
			Runtime.getRuntime().exec("java ordo.Daemon_dataNode 6060");
			local = InetAddress.getLocalHost().getHostName();
			System.out.println("Reducer lancé sur "+local);
	    	j.setReducer(local);
		} catch (Exception e1) {
			System.out.println("Erreur critique, vérifiez que aucun programme n'utilise le port 6060.");
			System.exit(1);
		}

        Scanner input = new Scanner(System.in);
        String cmd;
        boolean exit = false;

        System.out.println("Console MapReduce v0:");
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
                case "h":
                case "help":
                	InterfaceMapReduce.showHelp();
                    break;
                case "d":
                case "data":
                	try {

						Runtime.getRuntime().exec("ssh "+user+"@"+lcmd[1]+" cd "+Project.PATH+"src && java ordo.Daemon_dataNode 6060");
                		list_dataNode.add(lcmd[1]);
                		System.out.println("DataNode lancé sur "+lcmd[1]);

                	}catch (UnknownHostException e1){
                		System.out.println("Host Inconnu");

                	} catch (Exception e){
                		System.out.println("Erreur dans l'ajout d'un dataNode");
                	}
                	break;
                case "data_rm":
                	try {
                		Runtime.getRuntime().exec("ssh "+user+"@"+lcmd[1]+" pkill java");
                		list_dataNode.remove(lcmd[1]);
                		System.out.println("DataNode supprimé de "+lcmd[1]);

                	} catch (Exception e){
                		System.out.println("Erreur dans la suppresion d'un dataNode");
                	}
                	break;
                case "l":
                case "lancer":
                	try {
                		long t1 = System.currentTimeMillis();

                		j.setInputFname(lcmd[1]);
                		Integer i = 0;
                		for (String h : list_dataNode) {
                			j.addDataNode(i,h);
                			i = i + 1000;
                		}
                		j.startJob(new MyMapReduce());

                		long t2 = System.currentTimeMillis();
            	        System.out.println("time in ms ="+(t2-t1));
                	} catch (Exception e){
                		System.out.println("Erreur, utilisation: lancer $FILENAME ou l $FILENAME");
                		System.out.println("Les fragments doivent être nommé $FILENAME.0 , File$FILENAME.10 ,  File$FILENAME.20 , etc");
                		System.out.println("Pour pouvoir les différencier en local");
                	}

                	break;
                default:
                    System.out.println("Commande inconnue: " + cmd + "\n" + "Utilisez 'h' ou 'help' pour la liste des commandes disponibles.");
                    break;
            }
        } while (!exit);
        try {
        	for (String host : list_dataNode) {
    			Runtime.getRuntime().exec("ssh "+user+"@"+host+" pkill java");
    		}
    		Runtime.getRuntime().exec("pkill java");
        } catch (Exception e){}

        System.exit(0);
	}

}
