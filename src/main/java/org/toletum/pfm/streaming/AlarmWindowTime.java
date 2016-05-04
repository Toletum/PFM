package org.toletum.pfm.streaming;

import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AlarmWindowTime 
	extends 
	RichWindowFunction<TupleCrimeStreaming,
	Tuple2<String, Date>,Tuple,TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2591960925642041840L;
	
	@Override
	public void apply(Tuple key, TimeWindow window,
			Iterable<TupleCrimeStreaming> input,
			Collector<Tuple2<String, Date>> out) throws Exception {
		
		TupleCrimeStreaming ele;
		
		ele = input.iterator().next();
   		
		out.collect(new Tuple2<String, Date>(ele.f5, new Date()));
	}
	
	@Override
	public void open(Configuration parameters) throws Exception {
    	
	}

}
