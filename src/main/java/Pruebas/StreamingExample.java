package Pruebas;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer082;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.toletum.pfm.Config;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;


public class StreamingExample {
	
	public StreamingExample() throws Exception {
		Properties properties = new Properties();
		properties.put("bootstrap.servers", Config.KafkaServer);
		properties.put("zookeeper.connect", Config.ZooKeeperServer);
		properties.put("group.id", "CrimeStreaming");
		properties.put("topic", "test03");
		properties.put("auto.offset.reset", "latest");
		//properties.put("auto.offset.reset", "earliest");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer082<>(properties.getProperty("topic"), new SimpleStringSchema(), properties));
		
		/*
		messageStream//
		.keyBy(0)
		//.countWindow(2)
		.timeWindow(Time.seconds(5))
		//.window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(10)))
		//.countWindowAll(5)
		.sum(1)
		//.trigger(ContinuousEventTimeTrigger.of(Time.seconds(2)))
		//ContinuousProcessingTimeTrigger.of
		//.max(1)
		.print();
		*/
		
		messageStream
		.map(new Splitter())
		.keyBy(0)
		.timeWindow(Time.seconds(5))
		.trigger(CountTrigger.of(1))
		.sum(1)
		.print();
		
        env.execute("CrimeStreaming");
	}

	public static void main(String[] args) throws Exception {
		new StreamingExample();
	}
	
	public class Splitter implements MapFunction<String, Tuple2<Integer, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Integer, Integer> map(String arg0) {
			try {
				return new Tuple2<Integer,Integer>(new Integer(arg0), new Integer(1));
			} catch(java.lang.NumberFormatException e) {
				return new Tuple2<Integer,Integer>(new Integer(-1), new Integer(0));
			}
		}
		
	}

}
