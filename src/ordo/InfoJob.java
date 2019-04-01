package ordo;

import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

public class InfoJob implements Serializable {

  public double totalMapTime; // Temps d'exécution des maps en //
  public double reduceTime; // Temps d'exécution du reduce
  public double totalTime; // Temps d'exécution du job
  public Map<Integer, Double> mapTimes; // Temps d'exécution de la map sur chaque chunk (par index)

  public String fname;

  public InfoJob() {
    this.mapTimes = new HashMap<Integer, Double>();
  }


}
