package org.toletum.pfm.simulador;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import org.toletum.pfm.Utils;

public class SimuladorII {
	
	private TreeMap<String, LinkedList<String>> delitos = new TreeMap<String, LinkedList<String>>();

	
	private String makeKey(String data) {
		if(data.length()!=12) return null;
		
		String d = data.substring(2,4);
		String m = data.substring(0,2);
		String y = data.substring(4,8);
		String h = data.substring(8,12);
		
		return y+m+d+h;
	}
	
	public SimuladorII() {
		String str="";
		String key="";
		
		delitos.clear();
		
		try {
			Configuration conf = new Configuration();
			conf.set("fs.default.name", "hdfs://hadoop:9000/");
			
			FileSystem hdfs = FileSystem.get(conf);
			  
			System.out.println("Conectado a HADOOP");

			Path src = new Path("/BDP_RT/Based_Crime_RT.csv");

			
			FSDataInputStream inputStream = hdfs.open(src);
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
			
			while((str=reader.readLine()) != null) {
				
				key=makeKey(Utils.clearNumber(str.substring(0, 16)));
				
				if(key==null) continue;
				
				if (!delitos.containsKey(key)) {
					delitos.put(key, new LinkedList<String>());
				}

				delitos.get(key).add(str);
			}
			
			System.out.printf("Registros leidos: %d\n", delitos.size());
			
		} catch (IOException e) {
			e.printStackTrace();

		}
	}

	public static void main(String[] args) throws InterruptedException, ParseException {
		SimuladorII sim = new SimuladorII();
		
		sim.run();
		

	}

	private void run() throws ParseException, InterruptedException {
		String diaIni=delitos.firstKey();
		String crimeDate;
		String currentDate;
		String crime;
		LinkedList<String> lista;
		Iterator<String> valuesSetIterator;
		
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		
		SimpleDateFormat format1=new SimpleDateFormat("yyyyMMddHHmm");
		Date dt1=format1.parse(diaIni);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dt1);
		
		
		while(true) {
		
			crimeDate = format1.format(calendar.getTime());
			
			currentDate = format2.format(calendar.getTime());
			
			System.out.println(currentDate);
			
			
  		    lista = delitos.get(crimeDate);
			  
			if(lista!=null) {
				
			    valuesSetIterator = lista.iterator();
			
			
				while(valuesSetIterator.hasNext()){
				   crime = valuesSetIterator.next();
				   System.out.println("\t" + crime);
				}
			}
			
			
			calendar.add(Calendar.MINUTE, 1);
		
			Thread.sleep(1);
		}
	}
	
}
