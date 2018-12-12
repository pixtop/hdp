package formats;

import java.io.Serializable;
import java.lang.Exception;

public class HdfsResponse implements Serializable {

    private Exception error; // null si pas d'erreur

    private Serializable response; // error == null et response == null -> ACK

    public HdfsResponse(Serializable response, Exception error) {
      this.error = error;
      this.response = response;
    }

    public Exception getError() {
      return this.error;
    }

    public Serializable getResponse() {
      return this.response;
    }

}
