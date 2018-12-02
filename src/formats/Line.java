package formats;

import java.io.*;

public class Line implements Format {

    private OpenMode mode;
    private long index;
    private String fname;

    public Line(long index) {
        this.index = index;
    }

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
        return this.index;
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
        try {
            BufferedReader reader = new BufferedReader(new FileReader(this.fname));
            return new KV(Long.toString(this.index),  reader.readLine());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void write(KV record) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(this.fname));
            writer.write(record.v);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
