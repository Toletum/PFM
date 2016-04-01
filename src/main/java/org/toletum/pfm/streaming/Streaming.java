package org.toletum.pfm.streaming;

import java.util.Properties;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;


public class Streaming {
	private StreamExecutionEnvironment env;
	
	public Streaming(StreamExecutionEnvironment env) {
		this.env = env;
		
		/*
		Jedis jedis = new Jedis(Config.RedisServer);
		jedis.del(Config.RedisCrimes);
		jedis.close();
		
		sinkFunctionStatistics.CleanDB();
		*/

		
		Properties properties = new Properties();
	    properties.put("bootstrap.servers", Config.KafkaServer);
	    properties.put("zookeeper.connect", Config.ZooKeeperServer);
	    properties.put("group.id", "CrimeStreaming");
	    properties.put("topic", Config.KafkaTopicCrime);
	    //properties.put("auto.offset.reset", "latest");
	    properties.put("auto.offset.reset", "earliest");
		
		DataStream<String> messageStream = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		
		messageStream.print();
		

/*		
		KeyedStream<Tuple7<String, String, String, String, String, String, String>, Tuple> Crimes = messageStream.map(new StreamingCrimeSplitter ())
		.filter(new StreamingFilterFunction())
		.keyBy(1)
		;
		Crimes.addSink(new sinkFunction());
		Crimes.addSink(new sinkFunctionStatistics());
		
	
	    properties.put("topic", Config.KafkaTopicClock);
		DataStream<String> messageStreamClock = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		messageStreamClock
		.addSink(new sinkClockFunction());
		*/
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
	
}
