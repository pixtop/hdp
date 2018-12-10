package formats;

import java.io.Serializable;

public class HdfsResponse implements Serializable {

    private String error; // null si pas d'erreur

    private Serializable response; // error == null et response == null -> ACK

    public HdfsResponse(Serializable response, String error) {
      this.error = error;
      this.response = response;
    }

    public String getError() {
      return this.error;
    }

    public Serializable getResponse() {
      return this.response;
    }

}
