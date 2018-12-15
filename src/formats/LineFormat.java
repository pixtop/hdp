package formats;

import java.io.*;

public class LineFormat implements Format, Serializable {

	private static final long serialVersionUID = -9146586647773108023L;
	private String fname; // Nom du fichier
	private FileReader input;
	private BufferedReader br;
	private FileWriter output;
	private long index; // Nombre d'enregistrements lu

	public LineFormat() {
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
	public KV read() {
		try {
			String line = this.br.readLine();
			if(line == null)return null;
			return new KV(Long.toString(index++), line);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public void write(KV record) {
		if(output != null) {
			try {
				output.write(record.v + "\n");
				index ++;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/* Test */
	public static void main(String[] args) {
		try {
			LineFormat f = new LineFormat();
			f.setFname("test.txt");
			f.open(Format.OpenMode.W);
			for(int i = 0;i < 100; i ++)f.write(new KV(null,"Une ligne " + i));
			f.close();
			f.open(Format.OpenMode.R);
			for(int i = 0;i < 100; i ++) {
				KV r = f.read();
				System.out.println("Ligne " + r.k + " -> " + r.v);
			}
			f.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
