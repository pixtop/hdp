package hdfs;

import java.io.File;
import java.io.FileWriter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

import formats.Format;
import formats.HdfsQuery;
import formats.HdfsResponse;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;


public class HdfsClient {

    public static String nameNode = "localhost";
    private static final int taille_chunk_min = 1000; // taille minimale en octet d'un chunk (si taille du fichier inf à ce min, pas de division)

    private static void usage() {
        System.out.println("Usage: java hdfs/HdfsClient [-h <nameNode/dataNode_host>] <command>");
        System.out.println("Commands:");
        System.out.println("\tread <file_hdfs> [file_dst]");
        System.out.println("\twrite <line|kv> <file_src> [file_hdfs]");
        System.out.println("\tdelete <file_hdfs>");
        System.out.println("\tlist [file_name]"); // Liste des fichiers dans le nameNode
        System.out.println("\tchunks"); // Liste des fichiers dans le répertoire du dataNode
    }

    /** Send a request to the nameNode
    @param host address of the nameNode
    @param hq query to send
    @return HdfsResponse response of the nameNode
    @throws Exception if error in the query
    */
    public static HdfsResponse request(InetAddress host, HdfsQuery hq) throws Exception {
        // Ouverture socket avec un Node
        Socket s = new Socket(host, HdfsServer.port);
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        // Envoie une requête au Node
        oos.writeObject(hq);
        HdfsResponse response = (HdfsResponse) ois.readObject();
        // Fermeture de la socket
        s.close();
        if (response.getError() != null) throw response.getError();
        // retourne la réponse
        return response;
    }

    /**
     * @param hdfsFname remote hdfs file name to delete
     * @throws Exception if internal server error append
     */
    public static void HdfsDelete(String hdfsFname) throws Exception {
        InfoFichier file = HdfsClient.HdfsList(hdfsFname);
        Hashtable<Integer,Inet4Address> data_nodes = file.getChunks();

        // Suppresion chunks
        HdfsQuery query;
        for (Integer i : data_nodes.keySet()) {
            Inet4Address addr = data_nodes.get(i);
            // Demande d'un chunk
            query = new HdfsQuery(HdfsQuery.Command.DEL_CHUNK, hdfsFname, i, null);
            HdfsClient.request(addr, query);
        }

        // Suppresion enregistrement fichier
        query = new HdfsQuery(HdfsQuery.Command.DEL_FILE, hdfsFname);
        HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);
    }


    /**
     * @param fmt                file format
     * @param localFSSourceFname source of content file to write into hdfs
     * @param remoteHdfsName     remote hdfs file name
     * @param repFactor          replicate factor for redundancy (not used in Hidoop v0)
     * @throws Exception if internal server error append
     */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, String remoteHdfsName,
                                 int repFactor) throws Exception {

        if (remoteHdfsName == null) remoteHdfsName = (new File(localFSSourceFname)).getName();
        // Ouverture fichier
        Format reader = null;
        switch (fmt) {
            case LINE:
                reader = new LineFormat();
                break;
            case KV:
                reader = new KVFormat();
                break;
        }
        reader.setFname(localFSSourceFname);
        reader.open(Format.OpenMode.R);

        // Récupération all DataNodes
        HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_DATANODES, null);
        HdfsResponse response = HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);

        ArrayList data_nodes = (ArrayList) response.getResponse(); // ArrayList<Inet4Address>

        if (data_nodes.size() == 0) throw new Exception("Not a single DataNode in the system");

        long taille_chunk = reader.getSize() / data_nodes.size();
        taille_chunk = taille_chunk > taille_chunk_min ? taille_chunk : taille_chunk_min;

        //System.out.println("Taille fichier : " + reader.getSize() + " octets");
        //System.out.println("Nombre dataNodes : " + data_nodes.size());
        //System.out.println("Taille min chunks : " + taille_chunk_min);
        //System.out.println("Taille chunks selectionnée : " + taille_chunk);

        // Écriture des chunks
        InfoFichier newFile = new InfoFichier(remoteHdfsName, fmt);
        int i = 0;
        int TAILLE_MAX = 1000000;
        loop1:
        for(;;) {
            StringBuilder chk = new StringBuilder();
            long index = reader.getIndex();
            // Création du chunk par lecture du fichier

             for(long ki = 1; ki<= (taille_chunk/ TAILLE_MAX + ((taille_chunk % TAILLE_MAX== 0) ? 0 : 1)) ; ki++) {
	      	long k = ki*TAILLE_MAX;
	            for (long j = index; j < Long.min(index + taille_chunk,k); j = reader.getIndex()) {
	                KV rd = reader.read();
	                if (rd == null) break;
	                if (fmt == Format.Type.LINE) chk.append(rd.v);
	                else chk.append(rd.k).append(KV.SEPARATOR).append(rd.v);
	                chk.append("\n");
	            }
	            if (chk.length() > 0) {
	            	 if (k==1000000){
		                query = new HdfsQuery(HdfsQuery.Command.WRT_CHUNK, remoteHdfsName, (int)index, chk.toString()); // À faire : faire du param index un long
		                Inet4Address data_node = (Inet4Address) data_nodes.get(i);
		                Inet4Address backup_node;
		                if (i==data_nodes.size()-1) {
		                    backup_node = (Inet4Address) data_nodes.get(0);
		                } else {
		                    backup_node = (Inet4Address) data_nodes.get(i+1);
		                }
		                // Envoi chunk
		                HdfsClient.request(data_node, query); // Récupération ACK (à utiliser par la suite)
		                newFile.addChunk((int)index, data_node);
		                HdfsClient.request(backup_node, query); // Récupération ACK (à utiliser par la suite)
		                newFile.addBackup((int)index, backup_node);
		             } else {
		            	 query = new HdfsQuery(HdfsQuery.Command.EXT_CHUNK, remoteHdfsName, (int)index, chk.toString()); // À faire : faire du param index un long
		                Inet4Address data_node = (Inet4Address) data_nodes.get(i);
		                Inet4Address backup_node;
		                if (i==data_nodes.size()-1) {
		                    backup_node = (Inet4Address) data_nodes.get(0);
		                } else {
		                    backup_node = (Inet4Address) data_nodes.get(i+1);
		                }
		                // Envoi chunk
		                HdfsClient.request(data_node, query); // Récupération ACK (à utiliser par la suite)
		                HdfsClient.request(backup_node, query); // Récupération ACK (à utiliser par la suite)
		             }
	            } else break loop1;
            }

                i ++;

        }

        // Fermeture fichier
        reader.close();

        // Écriture sur NameNode
        query = new HdfsQuery(HdfsQuery.Command.WRT_FILE, newFile);
        HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);
    }


    /**
     * @param hdfsFname        file name to read into hdfs
     * @param localFSDestFname local copy destination file
     * @throws Exception if internal server error append
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws Exception {
        if (localFSDestFname == null) localFSDestFname = hdfsFname;

        InfoFichier f = HdfsClient.HdfsList(hdfsFname);

        Hashtable<Integer,Inet4Address> data_nodes = f.getChunks();

        // Récupération chunks
        StringBuilder content = new StringBuilder();
        List<Integer> indexs = new LinkedList<Integer>();
        for (Integer i : data_nodes.keySet()) indexs.add(i);
        Collections.sort(indexs);
        for (Integer i : indexs) {
            Inet4Address addr = data_nodes.get(i);

            // Demande d'un chunk
            HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_CHUNK, hdfsFname, i, null);
            HdfsResponse response = HdfsClient.request(addr, query);

            content.append((String)response.getResponse());
        }

        FileWriter file = new FileWriter(localFSDestFname);
        file.write(content.toString());
        file.close();
    }

    /**
    * List des fichiers enregistrés sur le NameNode
    * @return Array of String contenant les noms des fichiers
    * @throws Exception shit happens
    */
    public static String[] HdfsList() throws Exception {
      HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_FILE, null);
      return (String [])HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query).getResponse();
    }

    /**
    * Information sur un fichier enregistré
    * @return InfoFichier
    * @throws Exception si le fichier n'existe pas dans le hdfs
    */
    public static InfoFichier HdfsList(String fname) throws Exception {
      HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_FILE, fname);
      return (InfoFichier)HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query).getResponse();
    }

    /**
    * List des chunks enregistrés sur un DatNode
    * @param host DatNode en question
    * @return Array of String contenant les noms des chunks
    * @throws Exception shit happens
    */
    public static String[] HdfsChunks(String host) throws Exception {
      HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_CHUNK, null);
      return (String [])HdfsClient.request(InetAddress.getByName(host), query).getResponse();
    }

    public static void main(String[] args) {
        try {
            if (args.length >= 1) {
              int i = 0;
              if(args[0].equals("-h")) {
                if(args.length >= 2) {
                  HdfsClient.nameNode = args[1];
                  i += 2;
                } else {
                  System.err.println("You must specify a host");
                  return;
                }
              }
              switch(args[i]) {
                case "read":
                    if (args.length < i + 2) {
                        usage();
                        return;
                    }
                    HdfsRead(args[i+1], args.length < (i+3) ? null : args[i+2]);
                    break;
                case "delete":
                    if (args.length < i + 2) {
                        usage();
                        return;
                    }
                    HdfsDelete(args[i + 1]);
                    break;
                case "write":
                    Format.Type fmt;
                    if (args.length < i + 3) {
                        usage();
                        return;
                    }
                    if (args[i+1].equals("line")) fmt = Format.Type.LINE;
                    else if (args[i+1].equals("kv")) fmt = Format.Type.KV;
                    else {
                        usage();
                        return;
                    }
                    HdfsWrite(fmt, args[i+2], args.length < (i+4) ? null : args[i+3], 1);
                    break;
                case "list":
                    if(args.length < (i+2)) {
                      String[] l = HdfsClient.HdfsList();
                      if(l.length > 0) {
                        for(int j = 0;j < l.length; j ++) System.out.println(l[j]);
                      } else System.out.println("Looks empty");
                    } else {
                      InfoFichier f = HdfsClient.HdfsList(args[i + 1]);
                      System.out.println("File " + f.getNom() + ", format " + f.getFormat().toString());
                      Hashtable<Integer,Inet4Address> data_nodes = f.getChunks();
                      for(Integer j : data_nodes.keySet())
                        System.out.println("\tChunk index " + j.toString() + " on dataNode " + data_nodes.get(j).toString());
                    }
                    break;
                case "chunks":
                    String[] l = HdfsClient.HdfsChunks(HdfsClient.nameNode); // préciser l'host avec l'option -h
                    if(l.length > 0) {
                      for(int j = 0;j < l.length; j ++) System.out.println(l[j]);
                    } else System.out.println("Looks empty");
                    break;
                default:
                    usage();
              }
            } else HdfsClient.usage();
        } catch (Exception ex) {
            System.err.println("Error : " + ex.getMessage());
        }
    }
}
