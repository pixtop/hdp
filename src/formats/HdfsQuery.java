package formats;

import java.io.Serializable;

import hdfs.InfoFichier;

public class HdfsQuery implements Serializable {

    public enum Command {GET_FILE, // Récupération dataNodes gérant un fichier auprès du NameNode
                                   // ou récupération liste fichier du NameNode si pas de fichier spécifié
                        GET_CHUNK, // Récupération chunk d'un fichier auprès d'un DataNode
                                   // ou récupération de tous les chunks du DataNode si pas de fichier spécifié
                        GET_DATANODES, // Récupération liste data nodes sur le NameNode
                        WRT_FILE, // Écriture sur le NameNode
                        WRT_CHUNK, // Écriture sur un DataNode
                        EXT_CHUNK, // Permet de rendre un chunk plus long, en concatenant à la fin
                        DEL_CHUNK, // Suppression chunk sur DataNode
                        DEL_FILE // Suppresion file sur NameNode
                        }

    /* Utilisation de query + type de réponse
    * cmd :
    * GET_FILE : avec fname non null (String) -> répond avec un infoFichier
                 sans fname -> répond un Array of string contenant les noms des fichiers enregistrés
    * GET_CHUNK : avec fname String + chunk -> répond avec un String, contenu du chunk
                  sans fname -> répond un Array of String étant la liste des chunks enregistrés
    * GET_DATANODES : sans args -> répond une List<Inet4Address> stockant tous les dataNodes
    * WRT_FILE : avec fname = infoFichier -> répond un ack (réponse vide) confirmant l'enregistrement sur le NameNode
    * WRT_CHUNK : fname sous forme de String, chunk index du chunk, data contenu en String du chunk -> répond un ack
    * DEL_CHUNK : fname String + index -> répond un ack confirmant la suppresion
    * DEL_FILE : fname String -> répond un ack
    */

    private Serializable fname; // nom fichier (String) ou infoFichier pour un WRT_FILE (afin de rajouter le type)

    private Integer chunk; // index du chunk si besoin

    private Command cmd; // commande

    private Serializable data; // Map des DataNodes si cmd = WRT_CHUNK, index si cmd = GET_CHUNK, chunk (String) si WRT_CHUNK

    public HdfsQuery(Command cmd, Serializable fname) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = null;
      this.chunk = null;
    }

    public HdfsQuery(Command cmd, Serializable fname, Integer chunk, Serializable data) {
      this.fname = fname;
      this.cmd = cmd;
      this.data = data;
      this.chunk = chunk;
    }

    // Cas où fname est juste un nom
    public String getName() {
      return (String)this.fname;
    }

    // Cas où fname est un infoFichier
    public InfoFichier getInfo() {
      return (InfoFichier)this.fname;
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
