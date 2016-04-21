package org.toletum.pfm.streaming;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.toletum.pfm.Config;
import org.toletum.pfm.Utils;

import redis.clients.jedis.Jedis;


public class Streaming {
	private StreamExecutionEnvironment env;
	
	public Streaming(StreamExecutionEnvironment env) {
		this.env = env;
		
		Jedis jedis = new Jedis(Config.RedisServer);
		jedis.del(Config.RedisCrimes);
		jedis.close();
		
		SinkFunctionCrimeStadistics.CleanDB();

		
		Properties properties = new Properties();
	    properties.put("bootstrap.servers", Config.KafkaServer);
	    properties.put("zookeeper.connect", Config.ZooKeeperServer);
	    properties.put("group.id", "CrimeStreaming");
	    properties.put("topic", Config.KafkaTopicCrime);
	    //properties.put("auto.offset.reset", "latest");
	    properties.put("auto.offset.reset", "earliest");

		DataStream<String> messageStream = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		
		SingleOutputStreamOperator<Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String>> Crimes = messageStream
		.map(new CrimeMapStreaming())
		.keyBy(4)
		.filter(new StreamingFilterFunction());
		
	    /*
		Crimes.addSink(new SinkFunctionCrime()); // Posición GPS de delitos
		Crimes.addSink(new SinkFunctionCrimeStadistics()); // Actualización
		*/
/*
	    properties.put("topic", Config.KafkaTopicClock);
		DataStream<String> messageStreamClock = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		messageStreamClock.addSink(new SinkClockFunction());
		
		messageStreamClock
		.map(new ClockMap())
		.addSink(new SinkFuture());
		*/
//		.print();
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
