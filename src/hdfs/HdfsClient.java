package hdfs;
import formats.*;
import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.*;

import formats.*;


public class HdfsClient {

    public static String nameNode = "localhost";
    private static int taille_chunk = 1000; // Nombre d'enregistrement par chunk

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    public static void HdfsDelete(String hdfsFname) {

    }


    /**
    @throws Exception Si erreur (lire le message d'erreur, peut arriver un tas de trucs)
    */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname,
     int repFactor) throws Exception {
       // Ouverture fichier
       Format reader = null;
       switch(fmt) {
        case LINE:
          reader = new LineFormat();
          break;
        case KV:
          reader = new KVFormat();
          break;
       }
       reader.setFname(localFSSourceFname);
       reader.open(Format.OpenMode.R);
       // Ouverture socket avec DataNode
       Socket s = new Socket(HdfsClient.nameNode, HdfsServer.port);
       ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
       ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

       // Récupération all DataNodes
       HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_DATANODES, null);
       oos.writeObject(query);
       HdfsResponse response = (HdfsResponse)ois.readObject();
       if(response.getError() != null) throw response.getError();
       ArrayList data_nodes = (ArrayList)response.getResponse(); // ArrayList<Inet4Address>
       if(data_nodes.size() == 0)throw new Exception("Not a single DataNode in the system");

       // Écriture des chunks
       Hashtable<Integer, Inet4Address> used_nodes = new Hashtable<Integer, Inet4Address>();
       int i = 0;
       while(true) {
         int j;
         String chk = "";
         for(j = 0; j < HdfsClient.taille_chunk; j ++) {
           KV rd = reader.read();
           if(rd == null)break;
           if(fmt == Format.Type.LINE)chk = chk + rd.v + "\n";
           else chk = chk + rd.k + KV.SEPARATOR + rd.v + "\n";
         }
         query = new HdfsQuery(HdfsQuery.Command.WRT_CHUNK, localFSSourceFname, i * HdfsClient.taille_chunk, (Serializable)chk);
         Inet4Address data_node = (Inet4Address)data_nodes.get(i);
         Socket sd = new Socket(data_node, HdfsServer.port);
         ObjectOutputStream oosd = new ObjectOutputStream(s.getOutputStream());
         ObjectInputStream oisd = new ObjectInputStream(s.getInputStream());
         // Envoit chunk
         oosd.writeObject(query);
         response = (HdfsResponse)oisd.readObject(); // Récupération ACK
         if(response.getError() != null)throw response.getError();
         used_nodes.put(i * HdfsClient.taille_chunk, data_node);
         oosd.close();
         oisd.close();
         sd.close();
         i = (i + 1)%data_nodes.size();
         if(j != HdfsClient.taille_chunk)break;
       }

       // Écriture sur NameNode
       query = new HdfsQuery(HdfsQuery.Command.WRT_FILE, localFSSourceFname, (Serializable)used_nodes);
       oos.writeObject(query);
       response = (HdfsResponse)ois.readObject();
       if(response.getError() != null) throw response.getError();

       // Fermeture fichier
       reader.close();
       // Fermeture socket
       oos.close();
       ois.close();
       s.close();
     }

    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
      try {
        // Ouverture socket
        Socket s = new Socket(HdfsClient.nameNode, HdfsServer.port);
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

        // Obtention des datanodes gérant le fichier auprès du nameNode
        HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_FILE, hdfsFname);
        oos.writeObject(query);
        HdfsResponse response = (HdfsResponse)ois.readObject();
        if(response.getError() == null) {
          Hashtable chunks = (Hashtable)response.getResponse(); // Hashtable<Integer,Inet4Address>
          // TODO appel des dataNodes
        } else System.err.println("Error : " + response.getError());

        // Fermeture socket
        oos.close();
        ois.close();
        s.close();
      } catch (Exception e) {
        System.err.println("Error : " + e.getMessage());
      }
    }


    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],null); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write":
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

}
