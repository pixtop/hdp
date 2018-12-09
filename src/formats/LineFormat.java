package formats;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import config.Project;

public class LineFormat implements Format, Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = -9146586647773108023L;
	private OpenMode mode;
    private String fname;
    private int ligne_rendu;
    
    public LineFormat() {
    	ligne_rendu = 0;
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
	    		return new KV("",line);
	   	    }
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
    }

    @Override
    public void write(KV record) {
    	String a_ecrire = record.v+"\n";
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
    
  /*
    public static void main(String[] args) {
    	LineFormat l = new LineFormat();
    	l.setFname("test.txt");
    	l.write(l.read());
    	l.write(l.read());
    	
    } */
  
    
}
