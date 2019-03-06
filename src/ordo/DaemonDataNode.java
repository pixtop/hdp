package ordo;

import config.Project;
import formats.Format;
import map.MapReduce;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;

public class DaemonDataNode extends UnicastRemoteObject implements Daemon {

    private class Task {
        private final MapReduce mr;
        private final Format r, w;
        private final CallBack cb;

        private Task(MapReduce mr, Format r, Format w, CallBack cb) {
            this.mr = mr;
            this.r = r;
            this.w = w;
            this.cb = cb;
        }
    }

    private final ArrayList<Task> mapQ;

    private DaemonDataNode() throws RemoteException {
        mapQ = new ArrayList<>();
    }

    @Override
    public void runMap(MapReduce m, Format reader, Format writer, CallBack cb) throws IOException {
        synchronized (mapQ) {
            mapQ.add(new Task(m, reader, writer, cb));
            mapQ.notify();
        }
    }

    /**
     * Run map task while there is one task in the queue.
     * Wait if no task left.
     */
    private void run() {
        Task task;
        while (true) {
            synchronized (mapQ) {
                if (mapQ.isEmpty()) {
                    try {
                        mapQ.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                task = mapQ.get(0);
                mapQ.remove(0);
            }
            System.out.println("Map in progress");
            try {
                task.r.open(Format.OpenMode.R);
                task.w.open(Format.OpenMode.W);
                task.mr.map(task.r, task.w);
            } catch (IOException e) {
                System.out.println("Error: Could not execute mapping");
            }
            task.r.close();
            task.w.close();
            task.cb.mapDone();
            System.out.println("Map ended");
        }
    }

    static public void main (String[] args) {
        try {
            LocateRegistry.createRegistry(Project.RMI_PORT);
            DaemonDataNode daemon = new DaemonDataNode();
            Naming.rebind("//"+ InetAddress.getLocalHost().getHostAddress()+":"+Project.RMI_PORT+"/"+Project.RMI_DAEMON, daemon);
            daemon.run();
        } catch (RemoteException e) {
            System.out.println("Port is already used.");
            System.exit(1);
        } catch (UnknownHostException | MalformedURLException e) {
            System.out.println("Unknown host error.");
            System.exit(1);
        }
    }
}
