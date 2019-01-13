package ordo;

import java.util.Scanner;

import application.MyMapReduce;
import formats.Format;

public class InterfaceMapReduce {

	public static void showHelp(){
		System.out.print(
				"help, h: Affiche l'aide \n" +
                "exit, e: Quitte l'invité de commandes \n" +
                "data $HOSTNAME, d $HOSTNAME : Ajoute un dataNode sur $HOSTNAME \n" +
                "lancer $FILENAME, l $FILENAME : Lance un mapReduce sur la fichier d'entrée de nom $FILENAME \n"
        );
	}

	public static void main(String[] args){
		InterfaceMapReduce.showHelp();

		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setOutputformat(Format.Type.KV);

        try {
/*        	Process add_red = Runtime.getRuntime().exec("java ordo/Daemon_dataNode 6060");

    		if (add_red.exitValue()==0) {
    			System.out.println("Reducer lancé sur "+local);
    			j.setReducer(local);
    		} else {
    			System.out.print("Erreur critique");
    			System.exit(1);
    		}
*/
        	Process getlocal = Runtime.getRuntime().exec("hostname");
        	getlocal.waitFor();
        	byte[] b = new byte[256];
        	getlocal.getInputStream().read(b);
        	String local = new String(b,"UTF-8").split("\n")[0];

        	System.out.println("Reducer lancé sur "+local);
        	j.setReducer(local);


		} catch (Exception e) {
			e.printStackTrace();
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
                		/*
            			String user = System.getenv("USER");
                		Process add_data = Runtime.getRuntime().exec("ssh "+user+"@"+lcmd[1]+" java "+Project.PATH+"/src/ordo/Daemon_dataNode 6060");
                		add_data.waitFor();
                		System.out.println("DataNode lancé sur "+lcmd[1]);

                		System.out.println("ssh "+user+"@"+lcmd[1]+" java "+Project.PATH+"/src/ordo/Daemon_dataNode 6060");

                		if (add_data.exitValue()==0){
                			j.ajouterDataNode(lcmd[1]);
                		} else {
                			System.out.println("Erreur, ajout annulé");
                		}
                		*/
                		System.out.println("DataNode lancé sur "+lcmd[1]);
                	//	j.ajouterDataNode(lcmd[1]);

                	} catch (Exception e){
                		System.out.println("Erreur, utilisation: data $HOSTNAME ou d $HOSTNAME");
                	}
                	break;
                case "l":
                case "lancer":
                	try {
                		j.setInputFname(lcmd[1]);
                		j.startJob(new MyMapReduce());
                	} catch (Exception e){
                		System.out.println("Erreur, utilisation: lancer $FILENAME ou l $FILENAME");
                	}

                	break;
                default:
                    System.out.println("Commande inconnue: " + cmd + "\n" + "Utilisez 'h' ou 'help' pour la liste des commandes disponibles.");
                    break;
            }
        } while (!exit);
        System.exit(0);
	}

}
