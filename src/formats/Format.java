package formats;

import java.io.IOException;
import java.io.Serializable;

public interface Format extends FormatReader, FormatWriter, Serializable {
    public enum Type { LINE, KV };
    public enum OpenMode { R, W };

	public String getFname();
	public void setFname(String fname);

  // retourne la taille du fichier en octet
  public long getSize();

  // Nombre d'octets lu ou écrit jusqu'ici
  public long getIndex();

}
