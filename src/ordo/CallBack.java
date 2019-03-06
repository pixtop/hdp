package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

interface CallBack extends Remote {

    void mapDone() throws RemoteException;
}
