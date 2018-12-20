package hdfs;

import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.*;

import formats.*;


public class HdfsClient {

    public static String nameNode = "localhost";
    private static final int taille_chunk = 1000; // Nombre d'enregistrement par chunk

    private static void usage() {
        System.out.println("Usage: java hdfs/HdfsClient read <file_hdfs> [file_dst]");
        System.out.println("Usage: java hdfs/HdfsClient write <line|kv> <file_src> [file_hdfs]");
        System.out.println("Usage: java hdfs/HdfsClient delete <file_hdfs>");
    }

    private static HdfsResponse request(InetAddress host, HdfsQuery hq) throws Exception {
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

    private static Hashtable getDataNodes(String hdfsFname) throws Exception {
        // Demande chunks du fichier au NameNode
        HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_FILE, hdfsFname);
        HdfsResponse response = HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);
        return (Hashtable) response.getResponse();
    }

    /**
     * @param hdfsFname remote hdfs file name to delete
     * @throws Exception if internal server error append
     */
    public static void HdfsDelete(String hdfsFname) throws Exception {

        Hashtable data_nodes = HdfsClient.getDataNodes(hdfsFname); // Hashtable<Integer,Inet4Address>

        // Suppresion chunks
        HdfsQuery query;
        for (Object i : data_nodes.keySet()) {
            Inet4Address addr = (Inet4Address) data_nodes.get(i);
            // Demande d'un chunk
            query = new HdfsQuery(HdfsQuery.Command.DEL_CHUNK, hdfsFname, (Integer) i, null);
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
        // Récupération all DataNodes
        HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_DATANODES, null);
        HdfsResponse response = HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);

        ArrayList data_nodes = (ArrayList) response.getResponse(); // ArrayList<Inet4Address>
        if (data_nodes.size() == 0) throw new Exception("Not a single DataNode in the system");
        // System.out.println("Data Nodes récupérés");

        // Écriture des chunks
        Hashtable<Integer, Inet4Address> used_nodes = new Hashtable<>();
        int i = 0, index = 0;
        while (true) {
            int j;
            StringBuilder chk = new StringBuilder();
            for (j = 0; j < HdfsClient.taille_chunk; j++) {
                KV rd = reader.read();
                if (rd == null) break;
                if (fmt == Format.Type.LINE) chk.append(rd.v);
                else chk.append(rd.k).append(KV.SEPARATOR).append(rd.v);
                chk.append("\n");
            }
            if (j > 0) {
                // System.out.print("chunk " + index + " : " + chk);
                query = new HdfsQuery(HdfsQuery.Command.WRT_CHUNK, remoteHdfsName, index, chk.toString());
                Inet4Address data_node = (Inet4Address) data_nodes.get(i);

                // Envoi chunk
                HdfsClient.request(data_node, query); // Récupération ACK
                // System.out.println("Chunk " + index + " écrit");
                used_nodes.put(index, data_node);
                i = (i + 1) % data_nodes.size();
                index += HdfsClient.taille_chunk;
            }
            if (j != HdfsClient.taille_chunk) break;
        }

        // Fermeture fichier
        reader.close();

        // Écriture sur NameNode
        query = new HdfsQuery(HdfsQuery.Command.WRT_FILE, remoteHdfsName, used_nodes);
        HdfsClient.request(InetAddress.getByName(HdfsClient.nameNode), query);
        // System.out.println("Fichier écrit sur le NameNode");
    }


    /**
     * @param hdfsFname        file name to read into hdfs
     * @param localFSDestFname local copy destination file
     * @throws Exception if internal server error append
     */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws Exception {
        if (localFSDestFname == null) localFSDestFname = hdfsFname;

        Hashtable data_nodes = HdfsClient.getDataNodes(hdfsFname); // Hashtable<Integer,Inet4Address>

        // Récupération chunks
        StringBuilder content = new StringBuilder();
        List<Integer> indexs = new LinkedList<Integer>();
        for (Object i : data_nodes.keySet()) indexs.add((Integer) i);
        Collections.sort(indexs);
        for (Integer i : indexs) {
            Inet4Address addr = (Inet4Address) data_nodes.get(i);

            // Demande d'un chunk
            HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_CHUNK, hdfsFname, i, null);
            HdfsResponse response = HdfsClient.request(addr, query);

            content.append(response.getResponse());
        }

        FileWriter file = new FileWriter(localFSDestFname);
        file.write(content.toString());
        file.close();
    }


    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                usage();
                return;
            }

            switch (args[0]) {
                case "read":
                    HdfsRead(args[1], args.length < 3 ? null : args[2]);
                    break;
                case "delete":
                    HdfsDelete(args[1]);
                    break;
                case "write":
                    Format.Type fmt;
                    if (args.length < 3) {
                        usage();
                        return;
                    }
                    if (args[1].equals("line")) fmt = Format.Type.LINE;
                    else if (args[1].equals("kv")) fmt = Format.Type.KV;
                    else {
                        usage();
                        return;
                    }
                    HdfsWrite(fmt, args[2], args.length < 4 ? null : args[3], 1);
            }
        } catch (Exception ex) {
            System.err.println("Error : " + ex.getMessage());
        }
    }
}
