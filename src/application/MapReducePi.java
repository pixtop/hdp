package application;

import java.io.IOException;
import java.util.Random;

import formats.Format;
import formats.Format.OpenMode;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import formats.KVFormat;
import map.MapReduce;

public class MapReducePi implements MapReduce {
	private static final long serialVersionUID = 1L;

	// MapReduce program that computes pi using the Monte Carlo algorithm
	@Override
	public void map(FormatReader reader, FormatWriter writer) throws IOException   {
		Random rd = new Random();

		KV kv = reader.read();
		int nb = Integer.parseInt(kv.v);
		int Nb_in = 0;
		int Nb_out = 0;

		for (int i=0; i<nb; i++){
			float x = rd.nextFloat();
			float y = rd.nextFloat();
			if ((x*x+y*y)<1) {
				Nb_in++;
			} else {
				Nb_out++;
			}
		}

		writer.write(new KV("Nb_in",Integer.toString(Nb_in)));
		writer.write(new KV("Nb_out",Integer.toString(Nb_out)));
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) throws NumberFormatException, IOException {
		KV kv;
		int Nb_in = 0 ;
		int Nb_out = 0;

		while ((kv = reader.read()) != null) {
			if (kv.k.equals("Nb_in")) {
				Nb_in += Integer.parseInt(kv.v);
			} else {
				Nb_out += Integer.parseInt(kv.v);
			}
		}


		System.out.println(Nb_in +  "/"+Nb_out);
		float res = ((float) Nb_in/(Nb_out+Nb_in))*4;
		System.out.println(res);
		writer.write(new KV("res",Float.toString(res)));
	}

	public static void main(String[] args) throws IOException{
		// On choisis le nombre de points à calculer

		KVFormat reader = new KVFormat();
        reader.setFname("testNb");
        reader.open(Format.OpenMode.W);
        reader.write(new KV("", Integer.toString(1000000)));
        reader.close();

        // Premier map

		MapReducePi mp = new MapReducePi();
		KVFormat writer = new KVFormat();
		writer.setFname("MapOut");

		reader.open(Format.OpenMode.R);
		writer.open(Format.OpenMode.W);
		mp.map(reader, writer);
		reader.close();
		writer.close();

		// Deuxieme map

		KVFormat writer2 = new KVFormat();
		writer2.setFname("MapOut2");

		reader.open(Format.OpenMode.R);
		writer2.open(Format.OpenMode.W);
		mp.map(reader, writer2);
		reader.close();
		writer2.close();

		// Reduce

		KVFormat reader2 = new KVFormat();
		KVFormat writer3 = new KVFormat();
		reader2.setFname("MapOut");
		writer3.setFname("RedOut");

		reader2.open(Format.OpenMode.R);
		writer3.open(OpenMode.W);
		mp.reduce(reader2, writer3);
		reader2.close();
		writer3.close();
	}


}
