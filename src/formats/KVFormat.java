package formats;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.io.File;

public class KVFormat implements Format, Serializable{

		private static final long serialVersionUID = -6856607893995314001L;
    private String fname; // Nom du fichier
		private FileReader input;
		private FileWriter output;
		private BufferedReader br;
		private long index; // Nombre d'octets lu/écrit

    public KVFormat() {
			input = null;
			output = null;
			fname = null;
			index = 0;
    }

    @Override
		/**
		* @throws IOException if cannot open with that mode
		*/
    public void open(OpenMode mode) throws IOException {
			if(fname != null) {
					switch(mode) {
						case R:
								try {
									this.input = new FileReader(fname);
									this.br = new BufferedReader(input);
								} catch (FileNotFoundException e) {
									throw new IOException(e.getMessage());
								}
							break;
						case W:
								this.output = new FileWriter(fname);
							break;
					}
			} else throw new IOException("No file specified - use setFname()");
    }

    @Override
    public void close() {
			try {
				if(input != null)input.close();
				if(output != null)output.close();
			} catch (IOException e) {
				System.err.println(e.getMessage());
			}
			output = null;
			input = null;
			index = 0;
    }

    @Override
    public long getIndex() {
      return index;
    }

    @Override
    public String getFname() {
      return fname;
    }

    @Override
    public void setFname(String fname) {
			this.fname = fname;
    }

    @Override
		/**
		* @throws IOException si Erreur de format pendant la lecture
		*/
    public KV read() throws IOException {
				String line = this.br.readLine();
				if(line == null)return null;
				index += line.getBytes().length;
				String part[] = line.split(KV.SEPARATOR);
				if(part.length == 2)return new KV(part[0], part[1]);
				else throw new IOException("File is not in kv format");
    }

    @Override
    public void write(KV record) {
			if(output != null) {
				try {
					output.write(record.k + KV.SEPARATOR + record.v + "\n");
					index += record.k.getBytes().length + record.v.getBytes().length + 1 + KV.SEPARATOR.getBytes().length;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
    }

		@Override
		public long getSize() {
			return (new File(fname)).length();
		}

    public static void main(String[] args) {
			try {
	    	KVFormat f = new KVFormat();
	    	f.setFname("test.txt");
				f.open(Format.OpenMode.W);
				for(int i = 0;i < 100; i ++)f.write(new KV("Key " + i,"Val " + i));
				f.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
    }

}
