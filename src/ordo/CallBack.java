package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

interface CallBack extends Remote {

    void mapDone(Integer chunk, Double tps) throws RemoteException;
}
