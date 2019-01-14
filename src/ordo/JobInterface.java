package ordo;

import formats.Format;
import map.MapReduce;

public interface JobInterface {
// Méthodes requises pour la classe Job
	public void setInputFormat(Format.Type ft);
    public void setInputFname(String fname);

    public void startJob (MapReduce mr) throws ErreurJobException;
}