package exceptions;

import java.lang.Exception;

public class AlreadyExists extends Exception {

  public AlreadyExists(String msg) {
    super(msg);
  }

}
