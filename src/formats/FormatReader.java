package formats;

import java.io.IOException;

public interface FormatReader {
	/**
	* @throws IOException si Erreur de format pendant la lecture
	*/
	public KV read() throws IOException;
}
