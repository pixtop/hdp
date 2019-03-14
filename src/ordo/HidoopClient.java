package ordo;

import config.Project;
import map.MapReduce;
import application.MyMapReduce;
import hdfs.HdfsClient;
import formats.Format;
import formats.KVFormat;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;

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
  */
  public static void doJob(MapReduce mr, String input_name, String output_name) throws Exception {
    RessourceManager monitor = (RessourceManager) Naming.lookup("//" + HidoopClient.monitor.getHostAddress() + ':' + Project.RMI_PORT + '/' + Project.RMI_MONITOR);
    // Demande au moniteur de faire le map
    String hdfs_result = monitor.doJob(mr, input_name);
    // récupérer le fichier résultat en local
    String nameNode = monitor.getNameNode();
    if(nameNode.equals("localhost"))HdfsClient.nameNode = HidoopClient.monitor.getHostAddress(); // TODO : améliorer cette technique pt
    else HdfsClient.nameNode = nameNode;
    HdfsClient.HdfsRead(hdfs_result, hdfs_result);
    // Supprimer les fichiers tmp du hdfs
    HdfsClient.HdfsDelete(hdfs_result);
    // Faire le reduce en local
    Format r = new KVFormat(), w = new KVFormat();
    r.setFname(hdfs_result);
    w.setFname(output_name);
    r.open(Format.OpenMode.R);
    w.open(Format.OpenMode.W);
    mr.reduce(r, w);
    r.close();
    w.close();
  }

  public static void main(String[] args) {
    String fname = null, output = null;
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
        default:
          fname = args[i];
      }
    }
    if(output == null)output = fname + "-res";
    if(fname != null) {
      try {
        HidoopClient.doJob(new MyMapReduce(), fname, output);
      } catch(Exception e) {
        System.err.println(e.getMessage());
      }
    } else System.out.println("Usage: java HidoopClient <hdfs_file> [-o <local_result_file>] [-h <address_monitor>]");
  }

}
