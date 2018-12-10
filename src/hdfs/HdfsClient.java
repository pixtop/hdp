/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.*;
import java.net.*;
import java.io.*;
import java.lang.*;
import java.util.Hashtable;


public class HdfsClient {

    public static String nameNode = "localhost";

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }

    public static void HdfsDelete(String hdfsFname) {

    }

    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname,
     int repFactor) {

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
            ex.printStackTrace();
        }
    }

}
