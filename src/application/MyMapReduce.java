package application;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;

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
		try {
			while ((kv = reader.read()) != null) {
				StringTokenizer st = new StringTokenizer(kv.v);
				while (st.hasMoreTokens()) {
					String tok = st.nextToken();
					if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue()+1);
					else hm.put(tok, 1);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}


			for (String k : hm.keySet()) {
				writer.write(new KV(k,hm.get(k).toString()));
			}

		System.out.println("Fin map local");

	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
                Map<String,Integer> hm = new HashMap<>();
		KV kv;
		try {

				while ((kv = reader.read()) != null) {
					if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
					else hm.put(kv.k, Integer.parseInt(kv.v));
				}

		} catch (IOException e) {
			e.printStackTrace();
		}

			for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));



	}



}
