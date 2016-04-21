package org.toletum.pfm.OLD;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimulatorOLD {
	private Connection connection;
	private Statement st;
	
	private String diaIni = ""; 
	private String horaIni = "";
	private Calendar calendar;
	
	private int sleep = 1;
	
	private int uniqueID = 0;
	
	Producer<String, String> producer;
	
	public SimulatorOLD() {
		try {
			Class.forName("org.apache.drill.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			this.connection =DriverManager.getConnection("jdbc:drill:zk=zookeeper:2181");
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
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
	
	public void close() {
		try {
			this.connection.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void getIni() {
		String sql="select TO_DATE(CrimeDate, 'MM/dd/yyyy'), CrimeTime from dfs.toletum.`Based_Crime_RT.csv` order by TO_DATE(CrimeDate, 'MM/dd/yyyy'), CrimeTime  LIMIT 1";
		
		try {
			st = connection.createStatement();
			ResultSet rs = st.executeQuery(sql);
			if(rs.next()){
				this.diaIni = rs.getString(1);
				this.horaIni= rs.getString(2);
				
				SimpleDateFormat format1=new SimpleDateFormat("yyyy-MM-dd/HH:mm:ss");
				Date dt1=format1.parse(this.diaIni+"/"+this.horaIni);
				calendar = Calendar.getInstance();
				calendar.setTime(dt1);
			}
		} catch(SQLException e) {
			e.printStackTrace();
		} catch(ParseException e2) {
			e2.printStackTrace();
		}
	}
	
	public String toString() {
		return "diaIni: " + this.diaIni + "  horaIni: " + this.horaIni;
	}
	
	private void sendCrimes(Date dia) {
		String SQL;
		String kafkaString;
		
		SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd");
		String CrimeDate = format1.format(dia);
		
		SimpleDateFormat format2 = new SimpleDateFormat("hh:mm:%");
		String CrimeTime = format2.format(dia);
		
		
		kafkaString=CrimeDate+" "+CrimeTime.substring(0,5);
		
		producer.send(new ProducerRecord<String, String>("clock", "CLOCK", kafkaString));
		
		try {
			SQL = "select TO_DATE(CrimeDate,'MM/dd/yyyy'), CrimeTime,CrimeCode,Location,Description, " +
 				  " Weapon,Post,District,Neighborhood, `Location 1`,`Total Incidents` " +
				  " from dfs.toletum.`Based_Crime_RT.csv` " +
				  " where TO_DATE(CrimeDate,'MM/dd/yyyy') = TO_DATE('"+CrimeDate+"','yyyy-MM-dd') " +
				  " and CrimeTime Like '"+CrimeTime+"' ";
			
			this.st = this.connection.createStatement();
			
			ResultSet rs = this.st.executeQuery(SQL);
			while(rs.next()) {
				
				this.uniqueID++;
				
				kafkaString=""+uniqueID;
				kafkaString+=";";
				kafkaString+=rs.getString(1);
				kafkaString+=";";
				kafkaString+=rs.getString(2);
				kafkaString+=";";
				kafkaString+=rs.getString(8);
				kafkaString+=";";
				kafkaString+=rs.getString(10);
				kafkaString+=";";
				kafkaString+=rs.getString(11);
				
				System.out.println(kafkaString);
				
				producer.send(new ProducerRecord<String, String>("crimes", "CRIMES", kafkaString));
			}
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	public int getSleep() {
		return sleep;
	}

	public void setSleep(int sleep) {
		this.sleep = sleep;
	}

	public void startSimulation() throws InterruptedException {
		
		Thread slc = new Thread(new SimulatorOLD.LeeConfig(this));
        slc.start();

        
		while(true) {
			this.sendCrimes(calendar.getTime());
			
			calendar.add(Calendar.MINUTE, 1);
		
			Thread.sleep(this.sleep);
		}
	}
	

	public static void main(String[] args) throws InterruptedException, ParseException {
		SimulatorOLD sim = new SimulatorOLD();
		
		sim.getIni();
		System.out.println(sim);
		
		sim.startSimulation();
		
		sim.close();

	}
	
	private class LeeConfig implements Runnable {
		private SimulatorOLD parent;
		
	    private KafkaConsumer<String, String> consumer;
	    
	    public LeeConfig(SimulatorOLD parent) {
	    	this.parent = parent;
	    }

		@Override
		public void run() {
			System.out.println("LeeConfig: RUN");
			
	    	Properties props = new Properties();
	        props.put("bootstrap.servers", "kafka:9092");
	        props.put("group.id", "Simulator");
	        props.put("enable.auto.commit", "true");
	        props.put("auto.commit.interval.ms", "1000");
	        props.put("session.timeout.ms", "30000");
	        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

	        consumer = new KafkaConsumer<>(props);
	        consumer.subscribe(Arrays.asList("command"));
	        while (true) {
	            ConsumerRecords<String, String> records = consumer.poll(100);
	            for (ConsumerRecord<String, String> record : records) {
//	                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
	                if(record.value().startsWith("SIMULATORSLEEP")) {
	        			try {
	        				int i=Integer.parseInt(record.value().substring(14));
	        				System.out.println("LeeConfig: "+i);
	        				this.parent.setSleep(i);
	        			} catch(Exception ex) {
	        				System.out.println("LeeConfig: ???");
	        			}
	                }
	            }
	        }
			
		}
	}

}
