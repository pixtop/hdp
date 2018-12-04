package formats;

import java.io.Serializable;
import java.net.InetAddress;

public class Message implements Serializable {

    public enum Command {CMD_READ, CMD_WRITE, CMD_DELETE};

    private Command cmd;

    private InetAddress sourceAdress;

    private Format fmt;

    private int chunkHandle;

    public Command getCmd() {
        return cmd;
    }

    public void setCmd(Command cmd) {
        this.cmd = cmd;
    }

    public InetAddress getSourceAdress() {
        return sourceAdress;
    }

    public void setSourceAdress(InetAddress sourceAdress) {
        this.sourceAdress = sourceAdress;
    }

    public Format getFmt() {
        return fmt;
    }

    public void setFmt(Format fmt) {
        this.fmt = fmt;
    }

    public int getChunkHandle() {
        return chunkHandle;
    }

    public void setChunkHandle(int chunkHandle) {
        this.chunkHandle = chunkHandle;
    }
}
