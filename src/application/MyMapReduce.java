package application;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class MyMapReduce implements MapReduce, Serializable{


	/**
	 *
	 */
	private static final long serialVersionUID = 771177188680600097L;

	// MapReduce program that computes word counts
	@Override
	public void map(FormatReader reader, FormatWriter writer) {

		Map<String,Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			StringTokenizer st = new StringTokenizer(kv.v);
			while (st.hasMoreTokens()) {
				String tok = st.nextToken();
				if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue()+1);
				else hm.put(tok, 1);
			}
		}

		for (String k : hm.keySet()) {
			writer.write(new KV(k,hm.get(k).toString()));
		}
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
                Map<String,Integer> hm = new HashMap<>();
		KV kv;
		for (int i=0;i<10;i++) { // obligé de faire ça car ia des nulls qui se glissent dans le fichier???
			while ((kv = reader.read()) != null) {
				if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
				else hm.put(kv.k, Integer.parseInt(kv.v));
			}
		}
		for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
	}

	public static void main(String args[]) {
		Job j = new Job();
        j.setInputFormat(Format.Type.LINE);
        j.setInputFname(args[0]);
        j.setOutputformat(Format.Type.KV);
       long t1 = System.currentTimeMillis();
		j.startJob(new MyMapReduce());
		long t2 = System.currentTimeMillis();
        System.out.println("time in ms ="+(t2-t1));
        System.exit(0);
		}
}
