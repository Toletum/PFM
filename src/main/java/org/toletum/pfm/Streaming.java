package org.toletum.pfm;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import redis.clients.jedis.Jedis;


public class Streaming {
	private StreamExecutionEnvironment env;
	
	public Streaming(StreamExecutionEnvironment env) {
		this.env = env;
		
		
		Jedis jedis = new Jedis("database");
		jedis.del("JCS");
		jedis.close();

		
		Properties properties = new Properties();
	    properties.put("bootstrap.servers", Config.KafkaServer);
	    properties.put("zookeeper.connect", Config.ZooKeeperServer);
	    properties.put("group.id", "CrimeStreaming");
	    properties.put("topic", Config.KafkaTopicCrime);
	    properties.put("auto.offset.reset", "earliest");
		
		DataStream<String> messageStream = this.env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		
		messageStream.map(new StreamingCrimeSplitter ())
		.filter(new filterFunction())
		.addSink(new sinkFunction());
	}

	public static void main(String[] args) throws Exception {
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
		
		public sinkFunction() {
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 2859601213304525959L;

		@Override
		public void invoke(Tuple7<String, String, String, String, String, String, String> crime) throws Exception {
			System.out.println(crime.f0);
	    	jedis = new Jedis("database");
			
    		jedis.lpush("JCS", crime.f0);
    		jedis.ltrim("JCS", 0, 9);
    		
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
			// TODO Auto-generated method stub
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
