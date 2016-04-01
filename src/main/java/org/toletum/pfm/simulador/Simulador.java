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
import java.util.Properties;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


import org.toletum.pfm.Utils;

public class Simulador {
	private TreeMap<String, LinkedList<String>> delitos = new TreeMap<String, LinkedList<String>>();

	private String str="";
	private String key="";
	private int iSleep=250;
	
	Producer<String, String> producer;
	
	private String makeKey(String data) {
		if(data.length()!=12) return null;
		
		String d = data.substring(2,4);
		String m = data.substring(0,2);
		String y = data.substring(4,8);
		String h = data.substring(8,12);
		
		return y+m+d+h;
	}
	
	public void setiSleep(int iSleep) {
		this.iSleep = iSleep;
	}
	
	public Simulador() {
		delitos.clear();
	}
	
	private void loadData() throws IOException {
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
	}

	private void openKafka() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<String, String>(props);
	}
	
	private void run() throws ParseException, InterruptedException {
		String diaIni=delitos.firstKey();
		String crimeDate;
		String currentDate;
		String crime;
		LinkedList<String> lista;
		Iterator<String> valuesSetIterator;
		
		SimpleDateFormat format2=new SimpleDateFormat("yyyy-MM-dd HH:mm");
		SimpleDateFormat format1=new SimpleDateFormat("yyyyMMddHHmm");
		
		
		diaIni=delitos.firstKey();
		

		Date dt1=format1.parse(diaIni);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dt1);
		
		System.out.println(diaIni);
		
		while(true) {
		
			crimeDate = format1.format(calendar.getTime());
			
			currentDate = format2.format(calendar.getTime());
			
			System.out.print(currentDate);
			
			producer.send(new ProducerRecord<String, String>("clock", "CLOCK", currentDate));
			
  		    lista = delitos.get(crimeDate);
			  
			if(lista!=null) {
				valuesSetIterator = lista.iterator();
			
			
				while(valuesSetIterator.hasNext()){
					crime = valuesSetIterator.next();
					producer.send(new ProducerRecord<String, String>("crimes", "CRIMES", crime));
					System.out.print(".");
				}
			}
			
			calendar.add(Calendar.MINUTE, 1);
		
			Thread.sleep(iSleep);
			
			System.out.println();
		}
	}
	
	public static void main(String[] args) throws InterruptedException, ParseException, IOException {
		Simulador sim = new Simulador();
		
		if(args.length==1) {
			sim.setiSleep(Integer.parseInt(args[0]));
		}
			
		sim.loadData();
		sim.openKafka();
		sim.run();
	}

	
}
