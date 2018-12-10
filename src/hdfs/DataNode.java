package hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Paths;

public class DataNode {

    private File dir;

	public DataNode(String dir) throws NotDirectoryException {
	    File f = new File(dir);
	    if ( !f.exists() || !f.isDirectory() ) {
	        throw new NotDirectoryException("Le dossier n'existe pas");
        }
	    this.dir = f;
    }

    private static String readFile(String path) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded);
    }

	public String getChunk(String chunk) throws FileNotFoundException {
	    File[] files = this.dir.listFiles();
	    if (files == null) {
	        throw new FileNotFoundException();
        }
	    for (File f : files) {
	        if (f.isFile() && f.getName().equals(chunk) ) {
                try {
                    return readFile(f.getPath());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new FileNotFoundException();
    }

	public void delChunk(String chunk) {

    }

    public void addChunk(String chunk) {

    }
}
