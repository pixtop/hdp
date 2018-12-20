package formats;

import java.io.Serializable;

public class HdfsQuery implements Serializable {

    public enum Command {GET_FILE, // Récupération dataNodes gérant un fichier auprès du NameNode
                        GET_FILES, // Récupération de la liste des meta-fichiers du NameNode
                        GET_CHUNK, // Récupération chunk d'un fichier auprès d'un DataNode
                        GET_CHUNKS, // Récupération de tous les chunks enregistrer dans un DataNode
                        GET_DATANODES, // Récupération liste data nodes sur le NameNode
                        WRT_FILE, // Écriture sur le NameNode
                        WRT_CHUNK, // Écriture sur un DataNode
                        DEL_CHUNK, // Suppression chunk sur DataNode
                        DEL_FILE // Suppresion file sur NameNode
                        }

    private String fname; // nom fichier

    private Integer chunk; // index du chunk si besoin

    private Command cmd; // commande

    private Serializable data; // Map des DataNodes si cmd = WRT_CHUNK, index si cmd = GET_CHUNK, chunk (String) si WRT_CHUNK

    public HdfsQuery(Command cmd, String fname) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = null;
      this.chunk = null;
    }

    public HdfsQuery(Command cmd, String fname, Integer chunk, Serializable data) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = data;
      this.chunk = chunk;
    }

    public HdfsQuery(Command cmd, String fname, Serializable data) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = data;
      this.chunk = null;
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

    public Integer getChunk() {
      return this.chunk;
    }

}
