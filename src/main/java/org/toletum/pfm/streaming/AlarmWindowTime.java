package org.toletum.pfm.streaming;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AlarmWindowTime 
	extends 
	RichWindowFunction<Tuple9<Integer,String,Integer,Integer,Integer,String,Integer,String,String>,
	Tuple2<String, Long>,Tuple,TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2591960925642041840L;
	
	@Override
	public void apply(Tuple key, TimeWindow window,
			Iterable<Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String>> input,
			Collector<Tuple2<String, Long>> out) throws Exception {
		
		Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String> ele;
		
		ele = input.iterator().next();
   		
		out.collect(new Tuple2<String, Long>(ele.f5, new Long(System.currentTimeMillis())));
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
    	
	}

}
