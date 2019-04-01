package ordo;

import config.Project;
import map.MapReduce;
import application.MyMapReduce;
import hdfs.HdfsClient;
import formats.Format;
import formats.KVFormat;
import application.MapReducePi;


import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.io.File;
import java.lang.System;

/** Classe client Hidoop
* Lancer la commande sans args pour voir son usage
*/
public class HidoopClient {

  public static InetAddress monitor;

  /** Lancer un job sur hidoop
  * @param mr le MapReduce à lancer
  * @param input_name Nom du fichier hdfs sur lequel le lancer
  * @param output_name nom du fichier local où enregistrer le résultat
  * @throws Exception si quoi que ce soit se passe mal
  * @return InfoJob info exécution du job
  */
  public static InfoJob doJob(MapReduce mr, String input_name, String output_name) throws Exception {
    long totalTime = System.currentTimeMillis();
    RessourceManager monitor = (RessourceManager) Naming.lookup("//" + HidoopClient.monitor.getHostAddress() + ':' + Project.RMI_PORT + '/' + Project.RMI_MONITOR);
    // Demande au moniteur de faire le map
    InfoJob hdfs_result = monitor.doJob(mr, input_name);
    // récupérer le fichier résultat en local
    String nameNode = monitor.getNameNode();
    if(nameNode.equals("localhost"))HdfsClient.nameNode = HidoopClient.monitor.getHostAddress(); // TODO : améliorer cette technique pt
    else HdfsClient.nameNode = nameNode;
    HdfsClient.HdfsRead(hdfs_result.fname, hdfs_result.fname);
    // Supprimer les fichiers tmp du hdfs
    HdfsClient.HdfsDelete(hdfs_result.fname);
    // Faire le reduce en local
    Format r = new KVFormat(), w = new KVFormat();
    r.setFname(hdfs_result.fname);
    w.setFname(output_name);
    long reduceTime = System.currentTimeMillis();
    r.open(Format.OpenMode.R);
    w.open(Format.OpenMode.W);
    mr.reduce(r, w);
    r.close();
    w.close();
    reduceTime = System.currentTimeMillis() - reduceTime;
    (new File(hdfs_result.fname)).delete();
    totalTime = System.currentTimeMillis() - totalTime;
    hdfs_result.reduceTime = (double)reduceTime / 1000F;
    hdfs_result.totalTime = (double)totalTime / 1000F;
    return hdfs_result;
  }

  public static void usage() {
    System.out.println("Usage: java HidoopClient <hdfs_file> [-o <local_result_file>] [-h <address_monitor>] [-a <applicatiion>]");
    System.out.println("-a <application> : application = PI (calcul PI) ou WC (word count - par défaut)");
  }

  public static void main(String[] args) {
    String fname = null, output = null;
    MapReduce mr = new MyMapReduce();
    try {
      HidoopClient.monitor = InetAddress.getLocalHost();
    } catch(UnknownHostException e) {
      System.err.println("Error: Cannot get local host");
      System.exit(1);
    }
    for(int i = 0;i < args.length; i ++) {
      switch(args[i]) {
        case "-o":
          output = args[++i];
          break;
        case "-h":
          try {
            HidoopClient.monitor = InetAddress.getByName(args[++i]);
          } catch(UnknownHostException e) {
            System.err.println("Error: unknow host " + args[i]);
            System.exit(1);
          }
          break;
        case "-a":
          if(++i < args.length) {
            switch(args[i]) {
              case "PI":
                mr = new MapReducePi();
                break;
              case "WC":
                break;
              default:
                System.out.println("Unknow application " + args[i] + " (possibilities : PI, MC)");
                System.exit(1);
            }
          } else {
            HidoopClient.usage();
            System.exit(1);
          }
        default:
          fname = args[i];
      }
    }
    if(output == null)output = fname + "-res";
    if(fname != null) {
      try {
        InfoJob info = HidoopClient.doJob(mr, fname, output);
        System.out.println("MapReduce sur " + info.fname + " 1 seul Reduce, " + info.mapTimes.size() + " dataNodes");
        System.out.println("Temps total d'exécution : " + info.totalTime + " sec");
        System.out.println("Temps total des maps en parallèle : " + info.totalMapTime + " sec");
        for(Integer i : info.mapTimes.keySet()) {
          System.out.println("Temps Map sur chunk d'index " + i + " : " + info.mapTimes.get(i) + " sec");
        }
        System.out.println("Temps d'exécution du Reduce en local : " + info.reduceTime + " sec");
      } catch(Exception e) {
        System.err.println(e.getMessage());
      }
    } else HidoopClient.usage();
  }

}
