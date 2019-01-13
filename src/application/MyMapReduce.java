package application;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format.OpenMode;
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
			reader.open(OpenMode.R);
			while ((kv = reader.read()) != null) {
				StringTokenizer st = new StringTokenizer(kv.v);
				while (st.hasMoreTokens()) {
					String tok = st.nextToken();
					if (hm.containsKey(tok)) hm.put(tok, hm.get(tok).intValue()+1);
					else hm.put(tok, 1);
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			writer.open(OpenMode.W);
			for (String k : hm.keySet()) {
				writer.write(new KV(k,hm.get(k).toString()));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Fin map local");

	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
                Map<String,Integer> hm = new HashMap<>();
		KV kv;
		try {
			reader.open(OpenMode.R);
			for (int i=0;i<10;i++) { // obligé de faire ça car ia des nulls qui se glissent dans le fichier???
				while ((kv = reader.read()) != null) {
					if (hm.containsKey(kv.k)) hm.put(kv.k, hm.get(kv.k)+Integer.parseInt(kv.v));
					else hm.put(kv.k, Integer.parseInt(kv.v));
				}
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			writer.open(OpenMode.W);
			for (String k : hm.keySet()) writer.write(new KV(k,hm.get(k).toString()));
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}



}
