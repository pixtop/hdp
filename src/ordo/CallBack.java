package ordo;

import java.io.Serializable;

class CallBack implements Serializable {

    private int chunkSize;

    private int chunkMapped = 0;

    CallBack(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public synchronized void mapDone() {
        this.chunkMapped++;
        if (this.chunkMapped == chunkSize) {
            System.out.println("Map done.");
        }
    }
}
