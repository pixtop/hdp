package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class DataNode {

    private class KeepAlive implements Runnable {

        private Inet4Address addr;

        KeepAlive(Inet4Address addr) {
            this.addr = addr;
        }

        @Override
        public void run() {
            try {
                Socket s = new Socket(addr, SlaveKeepAlive.port);
                ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
                oos.writeObject(Inet4Address.getLocalHost());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private File dir;

    /**
     * @param dir chunks folder
     * @throws NotDirectoryException exception if directory is wrong
     */
    DataNode(String dir) throws NotDirectoryException {
        File f = new File(dir);

        if ( !f.exists() || !f.isDirectory() ) {
            throw new NotDirectoryException("Le dossier n'existe pas");
        }
        this.dir = f;
    }

    /**
     * @param dir chunks folder
     * @param nameNode Name node ip address
     * @throws IOException exception if there is connection problem
     */
	DataNode(String dir, Inet4Address nameNode) throws IOException {
	    this(dir);

        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.scheduleAtFixedRate(new KeepAlive(nameNode), 2, 5, TimeUnit.SECONDS);
    }

    private String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }

    private String makeName(String prefix, int suffix) {
	    return prefix + "." + suffix;
    }

    private File[] getFiles() throws FileNotFoundException {
        File[] files = this.dir.listFiles();
        if (files == null) {
            throw new FileNotFoundException();
        }
        return files;
    }

    /**
     * @param fname filename
     * @param chunk chunk number
     * @return contents
     * @throws IOException exception if there is not file found or I/O exception
     */
    String getChunk(String fname, int chunk) throws IOException {
	    String name = this.makeName(fname, chunk);
        File[] files = this.getFiles();
	    for (File f : files) {
	        if (f.isFile() && f.getName().equals(name) ) {
	            return this.readFile(f.getPath());
            }
        }
	    throw new FileNotFoundException();
    }

    /**
     * @param fname filename
     * @param chunk chunk number
     * @throws IOException exception if there is not file found or delete is impossible
     */
    void delChunk(String fname, int chunk) throws IOException {
        String name = this.makeName(fname, chunk);
        File[] files = this.getFiles();
        for (File f : files) {
            if (f.isFile() && f.getName().equals(name)) {
                if (f.delete()) {
                    return;
                }
                else {
                    throw new IOException();
                }
            }
        }
        throw new FileNotFoundException();
    }

    /**
     * @param fname filename
     * @param chunk chunk number
     * @param content content of the file
     * @throws IOException exception if writing is impossible
     */
    void addChunk(String fname, int chunk, String content) throws IOException {
        FileWriter file = new FileWriter(this.dir.getPath() + '/' + makeName(fname, chunk));
        file.write(content);
        file.close();
    }

    /**
     * @return array of file names stored
     */
    String[] showChunks() {
        try {
            File[] files = this.getFiles();
            int len = files.length;
            String[] data = new String[len];
            for (int i=0; i<len; i++) {
                data[i] = files[i].getName();
            }
            return data;
        } catch (FileNotFoundException e) {
            return new String[0];
        }
    }
}
