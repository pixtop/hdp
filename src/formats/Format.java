package formats;

import java.io.IOException;
import java.io.Serializable;

public interface Format extends FormatReader, FormatWriter, Serializable {
    public enum Type { LINE, KV };
    public enum OpenMode { R, W };

  /**
  @throws IOException Si erreur dans l'ouverture du fichier
  * Voir utilisation dans main de LineFormat et KVFormat
  */

	public long getIndex();
	public String getFname();
	public void setFname(String fname);

}
