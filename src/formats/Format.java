package formats;

import java.io.Serializable;
import java.io.IOException;

public interface Format extends FormatReader, FormatWriter, Serializable {
    public enum Type { LINE, KV };
    public enum OpenMode { R, W };

  /**
  @throws IOException Si erreur dans l'ouverture du fichier
  */
	public void open(OpenMode mode) throws IOException;
	public void close();
	public long getIndex();
	public String getFname();
	public void setFname(String fname);

}
