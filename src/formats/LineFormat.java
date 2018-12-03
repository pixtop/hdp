package formats;

import java.io.*;

public class LineFormat implements Format {

    private OpenMode mode;
    private String fname;

    @Override
    public void open(OpenMode mode) {
        this.mode = mode;
    }

    @Override
    public void close() {
        this.mode = null;
    }

    @Override
    public long getIndex() {
        return 0;
    }

    @Override
    public String getFname() {
        return this.fname;
    }

    @Override
    public void setFname(String fname) {
        this.fname = fname;
    }

    @Override
    public KV read() {
        return null;
    }

    @Override
    public void write(KV record) {

    }
}
