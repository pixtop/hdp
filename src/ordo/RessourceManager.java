package ordo;

import exceptions.ErreurJobException;
import formats.Format;
import map.MapReduce;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.net.InetAddress;

public interface RessourceManager extends Remote {

    /** Réaliser un mapReduce sur un fichier du hdfs
    @param mr Le mapReduce à exécuter
    @param fname Nom du fichier hdfs sur lequel l'exécuter
    @return String nom du fichier résultat des map, à recupérer par hdfs en local pour faire le reduce
    */
    public String doJob(MapReduce mr, String fname) throws RemoteException, ErreurJobException;

    /** Récupérer l'adresse du nameNode
    @return String adresse du nameNode
    */
    public String getNameNode() throws RemoteException;

}
