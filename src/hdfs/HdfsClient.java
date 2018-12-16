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
        System.out.println("Usage: java hdfs/HdfsClient read <file_hdfs> [file_dst]");
        System.out.println("Usage: java hdfs/HdfsClient write <line|kv> <file_src> [file_hdfs]");
        System.out.println("Usage: java hdfs/HdfsClient delete <file_hdfs>");
    }

    /**
    @throws Exception Si erreur (lire le message d'erreur, peut arriver un tas de trucs)
    */
    public static void HdfsDelete(String hdfsFname) throws Exception {
      // Ouverture socket avec DataNode
      Socket s = new Socket(HdfsClient.nameNode, HdfsServer.port);
      ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
      ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

      // Demande chunks du fichier au NameNode
      HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_FILE, hdfsFname);
      oos.writeObject(query);
      HdfsResponse response = (HdfsResponse)ois.readObject();

      // Fermeture socket
      oos.close();
      ois.close();
      s.close();

      if(response.getError() != null)throw response.getError();
      Hashtable data_nodes = (Hashtable)response.getResponse(); // Hashtable<Integer,Inet4Address>

      // Suppresion chunks
      for(Object i : data_nodes.keySet()) {
        Inet4Address addr = (Inet4Address)data_nodes.get(i);
        s = new Socket(addr, HdfsServer.port);
        oos = new ObjectOutputStream(s.getOutputStream());
        ois = new ObjectInputStream(s.getInputStream());

        // Demande d'un chunk
        query = new HdfsQuery(HdfsQuery.Command.DEL_CHUNK, hdfsFname, (Integer)i, null);
        oos.writeObject(query);
        response = (HdfsResponse)ois.readObject();

        // Fermeture
        oos.close();
        ois.close();
        s.close();

        if(response.getError() != null) throw response.getError();
      }

      // Ouverture socket avec DataNode
      s = new Socket(HdfsClient.nameNode, HdfsServer.port);
      oos = new ObjectOutputStream(s.getOutputStream());
      ois = new ObjectInputStream(s.getInputStream());

      // Suppresion enregistrement fichier
      query = new HdfsQuery(HdfsQuery.Command.DEL_FILE, hdfsFname);
      oos.writeObject(query);
      response = (HdfsResponse)ois.readObject();

      // Fermeture socket
      oos.close();
      ois.close();
      s.close();

      if(response.getError() != null)throw response.getError();

    }


    /**
    @throws Exception Si erreur (lire le message d'erreur, peut arriver un tas de trucs)
    */
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, String remoteHdfsName,
     int repFactor) throws Exception {
       if(remoteHdfsName == null)remoteHdfsName = (new File(localFSSourceFname)).getName();
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

       // Fermeture socket
       oos.close();
       ois.close();
       s.close();

       if(response.getError() != null) throw response.getError();
       ArrayList data_nodes = (ArrayList)response.getResponse(); // ArrayList<Inet4Address>
       if(data_nodes.size() == 0)throw new Exception("Not a single DataNode in the system");
       // System.out.println("Data Nodes récupérés");

       // Écriture des chunks
       Hashtable<Integer, Inet4Address> used_nodes = new Hashtable<Integer, Inet4Address>();
       int i = 0, index = 0;
       while(true) {
         int j;
         String chk = "";
         for(j = 0; j < HdfsClient.taille_chunk; j ++) {
           KV rd = reader.read();
           if(rd == null)break;
           if(fmt == Format.Type.LINE)chk = chk + rd.v + "\n";
           else chk = chk + rd.k + KV.SEPARATOR + rd.v + "\n";
         }
         if(j > 0) {
           // System.out.print("chunk " + index + " : " + chk);
           query = new HdfsQuery(HdfsQuery.Command.WRT_CHUNK, remoteHdfsName, index, (Serializable)chk);
           Inet4Address data_node = (Inet4Address)data_nodes.get(i);
           Socket sd = new Socket(data_node, HdfsServer.port);
           ObjectOutputStream oosd = new ObjectOutputStream(sd.getOutputStream());
           ObjectInputStream oisd = new ObjectInputStream(sd.getInputStream());
           // Envoit chunk
           oosd.writeObject(query);
           response = (HdfsResponse)oisd.readObject(); // Récupération ACK
           if(response.getError() != null)throw response.getError();
           // System.out.println("Chunk " + index + " écrit");
           used_nodes.put(index, data_node);
           oosd.close();
           oisd.close();
           sd.close();
           i = (i + 1)%data_nodes.size();
           index += HdfsClient.taille_chunk;
         }
         if(j != HdfsClient.taille_chunk)break;
       }

       // Fermeture fichier
       reader.close();

       // Ouverture socket avec DataNode
       s = new Socket(HdfsClient.nameNode, HdfsServer.port);
       oos = new ObjectOutputStream(s.getOutputStream());
       ois = new ObjectInputStream(s.getInputStream());

       // Écriture sur NameNode
       query = new HdfsQuery(HdfsQuery.Command.WRT_FILE, remoteHdfsName, (Serializable)used_nodes);
       oos.writeObject(query);
       response = (HdfsResponse)ois.readObject();
       if(response.getError() != null) throw response.getError();
       // System.out.println("Fichier écrit sur le NameNode");

       // Fermeture socket
       oos.close();
       ois.close();
       s.close();
     }

    /**
    @throws Exception Si erreur (lire le message d'erreur, peut arriver un tas de trucs, encore)
    */
    public static void HdfsRead(String hdfsFname, String localFSDestFname) throws Exception {
      if(localFSDestFname == null)localFSDestFname = hdfsFname;

      // Ouverture socket avec DataNode
      Socket s = new Socket(HdfsClient.nameNode, HdfsServer.port);
      ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
      ObjectInputStream ois = new ObjectInputStream(s.getInputStream());

      // Demande chunks du fichier au NameNode
      HdfsQuery query = new HdfsQuery(HdfsQuery.Command.GET_FILE, hdfsFname);
      oos.writeObject(query);
      HdfsResponse response = (HdfsResponse)ois.readObject();

      // Fermeture socket
      oos.close();
      ois.close();
      s.close();

      if(response.getError() != null)throw response.getError();
      Hashtable data_nodes = (Hashtable)response.getResponse(); // Hashtable<Integer,Inet4Address>

      // Récupération chunks
      String content = "";
      List<Integer> indexs = new LinkedList<Integer>();
      for(Object i : data_nodes.keySet())indexs.add((Integer)i);
      Collections.sort(indexs);
      for(Integer i : indexs) {
        Inet4Address addr = (Inet4Address)data_nodes.get(i);
        s = new Socket(addr, HdfsServer.port);
        oos = new ObjectOutputStream(s.getOutputStream());
        ois = new ObjectInputStream(s.getInputStream());

        // Demande d'un chunk
        query = new HdfsQuery(HdfsQuery.Command.GET_CHUNK, hdfsFname, i, null);
        oos.writeObject(query);
        response = (HdfsResponse)ois.readObject();

        // Fermeture
        oos.close();
        ois.close();
        s.close();

        // ajout chunk
        if(response.getError() != null)throw response.getError();
        content = content + (String)response.getResponse();
      }

      FileWriter file = new FileWriter(localFSDestFname);
      file.write(content);
      file.close();
    }


    public static void main(String[] args) {
        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read":
                HdfsRead(args[1],args.length<3 ? null : args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write":
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],args.length<4 ? null : args[3],1);
            }
        } catch (Exception ex) {
            System.err.println("Error : " + ex.getMessage());
        }
    }

}
