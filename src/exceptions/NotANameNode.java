package exceptions;

import java.lang.Exception;

public class NotANameNode extends Exception {

  public NotANameNode(String msg) {
    super(msg);
  }

}
