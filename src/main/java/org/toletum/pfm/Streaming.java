package org.toletum.pfm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import redis.clients.jedis.Jedis;


import javax.sql.DataSource;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;


public class Streaming {
	private StreamExecutionEnvironment env;
	
	private DataSource setupDataSource() {
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory("jdbc:neo4j://database:7474/",null);
		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		ObjectPool<PoolableConnection> connectionPool = new GenericObjectPool<>(poolableConnectionFactory);
		poolableConnectionFactory.setPool(connectionPool);
		PoolingDataSource<PoolableConnection> dataSource = new PoolingDataSource<>(connectionPool);
		
		return dataSource;
	}
	
	public Streaming(StreamExecutionEnvironment env) {
		this.env = env;
		
		
		Jedis jedis = new Jedis(Config.RedisServer);
		jedis.del(Config.RedisCrimes);
		jedis.close();

		
		Properties properties = new Properties();
	    properties.put("bootstrap.servers", Config.KafkaServer);
	    properties.put("zookeeper.connect", Config.ZooKeeperServer);
	    properties.put("group.id", "CrimeStreaming");
	    properties.put("topic", Config.KafkaTopicCrime);
	    //properties.put("auto.offset.reset", "latest");
	    properties.put("auto.offset.reset", "earliest");
		
		DataStream<String> messageStream = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));

	    properties.put("topic", Config.KafkaTopicClock);
		DataStream<String> messageStreamClock = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		
		KeyedStream<Tuple7<String, String, String, String, String, String, String>, Tuple> Crimes = messageStream.map(new StreamingCrimeSplitter ())
		.filter(new filterFunction())
		.keyBy(1);
		
		Crimes.addSink(new sinkFunction());
		
		messageStreamClock
		.map(new MapFunction<String, Tuple2<Integer, String>>() {
		    /**
			 * 
			 */
			private static final long serialVersionUID = -1736125446606461328L;

			@Override
		    public Tuple2<Integer, String> map(String value) throws Exception {
		        return new Tuple2<Integer, String>(new Integer(0), value);
		    }
		})
		.keyBy(1)
		.addSink(new sinkClockFunction());
	}

	public static void main(String[] args) throws Exception {
		try {
			Class.forName("org.neo4j.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        @SuppressWarnings("unused")
		Streaming streaming = new Streaming(env);
        
        env.execute("CrimeStreaming");
	}
	
	private static class filterFunction
	  implements FilterFunction<Tuple7<String, String, String, String, String, String,String>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 6322140487099573422L;

		@Override
		public boolean filter(Tuple7<String, String, String, String, String, String, String> crime) throws Exception {
			
			return !crime.f1.equals("ERROR");
		}
	}
	
	private static class sinkFunction
	  implements SinkFunction<Tuple7<String, String, String, String, String, String,String>> {
	    private Jedis jedis;
	    
		/**
		 * 
		 */
		private static final long serialVersionUID = 2859601213304525959L;

		@Override
		public void invoke(Tuple7<String, String, String, String, String, String, String> crime) throws Exception {
			System.out.println(crime.f0);
	    	jedis = new Jedis(Config.RedisServer);
			
    		jedis.lpush(Config.RedisCrimes, crime.f0);
    		jedis.ltrim(Config.RedisCrimes, 0, 9);
    		
    		jedis.close();
    		
    		Connection con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
    		
			Statement stmt = con.createStatement();
			stmt.execute("MATCH (R: Root) CREATE UNIQUE (R) -[:X]-> (Prueba: Prueba { name: 'Root' })");
			stmt.close();
			
    		con.close();
    		/*
    		Connection conn = dataSource.getConnection();
    		*/
		}
	}

	private static class sinkClockFunction
	  implements SinkFunction<Tuple2<Integer, String>> {
	    private Jedis jedis;
		
		public sinkClockFunction() {
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 2859601213304525959L;

		@Override
		public void invoke(Tuple2<Integer, String> fechaHora) throws Exception {
			System.out.println(fechaHora.f1);
	    	jedis = new Jedis(Config.RedisServer);
			
	    	jedis.set(Config.RedisClock, fechaHora.f1);
	    	
	    	jedis.close();
		}
	}
	
	private static class StreamingCrimeSplitter 
	  implements MapFunction<String, Tuple7<String, String, String, String, String, String,String>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -9139495496732679596L;

		@Override
		public Tuple7<String, String, String, String, String, String, String> map(String value) throws Exception {
			String []values = value.split(";");
			
			Tuple7<String, String, String, String, String, String,String> t; 
			try {
				t = new Tuple7<String, String, String, String, String, String,String>(value, values[0],values[1],values[2],values[3],values[4],values[5]);
			} catch(Exception ex) {
				t = new Tuple7<String, String, String, String, String, String,String>(value, "ERROR","","","","","");
			}
			
			return t;
		}
		
	}
}
