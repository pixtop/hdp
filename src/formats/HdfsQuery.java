package formats;

import java.io.Serializable;

public class HdfsQuery implements Serializable {

    public enum Command {GET_FILE, // Récupération dataNodes gérant un fichier auprès du nameNode
                        GET_CHUNK, // Récupération chunk d'un fichier auprès d'un DataNode
                        GET_DATANODES, // Récupération liste data nodes sur le NameNode
                        WRT_FILE, // Écriture sur le NameNode
                        WRT_CHUNK // Écriture sur un DataNode
                        };

    private String fname;

    private Command cmd;

    private Serializable data;

    public HdfsQuery(Command cmd, String fname) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = null;
    }

    public HdfsQuery(Command cmd, String fname, Serializable data) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = data;
    }

    public String getName() {
      return this.fname;
    }

    public Command getCmd() {
      return this.cmd;
    }

    public Serializable getData() {
      return this.data;
    }

}
