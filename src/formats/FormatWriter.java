package formats;

import java.io.IOException;

import formats.Format.OpenMode;

public interface FormatWriter {
	public void write(KV record);
	public void open(OpenMode mode) throws IOException;
	public void close();
}
