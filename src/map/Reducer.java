package map;

import java.io.IOException;
import java.io.Serializable;

import formats.FormatReader;
import formats.FormatWriter;

public interface Reducer extends Serializable {
	public void reduce(FormatReader reader, FormatWriter writer) throws NumberFormatException, IOException;
}
