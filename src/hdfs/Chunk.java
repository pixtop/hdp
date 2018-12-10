package hdfs;
import java.net.Inet4Address;
import java.util.List;


public class Chunk {

	private Inet4Address main;
	private List<Inet4Address> backups;

	public Chunk(Inet4Address main,List<Inet4Address> backups){
		this.main = main;
		this.backups = backups;
	}

	public Inet4Address getMain() {
		return main;
	}

	// Change le main et supprime le nouveau main du backup si il est déjà
	public void setMain(Inet4Address main) {
		this.main = main;
		if (backups.contains(main)) {
			backups.remove(main);
		}
	}

	public List<Inet4Address> getBackups() {
		return backups;
	}

	// Ajoute un nouveau backup si il n'est pas déjà présent
	public void addBackup(Inet4Address nv_back) {
		if (!backups.contains(nv_back)) {
			backups.add(nv_back);
		}
	}

	// Supprime un backup si il est pas présent dans la liste des backups
		public void remBackup(Inet4Address back) {
			if (backups.contains(back)) {
				backups.remove(back);
			}
		}



}
