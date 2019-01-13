package formats;

import java.io.IOException;

import formats.Format.OpenMode;

public interface FormatReader {
	/**
	* @throws IOException si Erreur de format pendant la lecture
	*/
	public KV read() throws IOException;
	public void open(OpenMode mode) throws IOException;
	public void close();
}
