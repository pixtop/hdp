package formats;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import config.Project;

public class KVFormat implements Format, Serializable{


	/**
	 *
	 */
	private static final long serialVersionUID = -6856607893995314001L;
	private OpenMode mode;
    private String fname;
    private int ligne_rendu;

    public KVFormat() {
    }

    @Override
    public void open(OpenMode mode) {
        this.mode = mode;
    }

    @Override
    public void close() {
        this.mode = null;
    }

    @Override
    public long getIndex() {
        return 0;
    }

    @Override
    public String getFname() {
        return this.fname;
    }

    @Override
    public void setFname(String fname) {
        this.fname = fname;
    }

    @Override
    public KV read() {
    	System.out.println("Accesing:"+Project.PATH+"data\\"+fname);
    	BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(Project.PATH+"data\\"+fname));
			for (int i=0;i<ligne_rendu;i++) {
				br.readLine();
	    	}
	   	    String line = br.readLine();
	   	    this.ligne_rendu++;
	   	    if (line==null) {
	   	    	br.close();
	   	    	return null;
	   	    }else {
	   	    	br.close();
	   	    	String[] parts = line.split(KV.SEPARATOR);
	    		return new KV(parts[0],parts[1]);
	   	    }
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }

    @Override
    public void write(KV record) {
    	String a_ecrire = record.k+KV.SEPARATOR+record.v+"\n";
		try {
			Files.write(Paths.get(Project.PATH+"data/"+fname+"-rec"), a_ecrire.getBytes(), StandardOpenOption.APPEND);
		} catch (NoSuchFileException e) {
			try {
				Files.write(Paths.get(Project.PATH+"data/"+fname+"-rec"), a_ecrire.getBytes(), StandardOpenOption.CREATE_NEW);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

    /* TESTS
    public static void main(String[] args) {
    	LineFormat l = new LineFormat();
    	l.setFname("test.txt");
    	l.write(l.read());
    	l.write(l.read());

    } */
}
