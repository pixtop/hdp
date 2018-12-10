package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class DataNode {

    private File dir;

	public DataNode(String dir) throws NotDirectoryException {
	    File f = new File(dir);
	    if ( !f.exists() || !f.isDirectory() ) {
	        throw new NotDirectoryException("Le dossier n'existe pas");
        }
	    this.dir = f;
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
     * @throws FileNotFoundException exception if there is not file found
     */
	public String getChunk(String fname, int chunk) throws FileNotFoundException {
	    String name = this.makeName(fname, chunk);
        File[] files = this.getFiles();
	    for (File f : files) {
	        if (f.isFile() && f.getName().equals(name) ) {
                try {
                    return this.readFile(f.getPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new FileNotFoundException();
    }

    /**
     * @param fname filename
     * @param chunk chunk number
     * @throws IOException exception if there is not file found or delete is impossible
     */
	public void delChunk(String fname, int chunk) throws IOException {
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
    public void addChunk(String fname, int chunk, String content) throws IOException {
        Path file = Paths.get(this.makeName(fname, chunk));
        Files.write(file, Collections.singleton(content));
    }
}
