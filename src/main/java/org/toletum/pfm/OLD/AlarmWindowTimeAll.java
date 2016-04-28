package org.toletum.pfm.OLD;

import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AlarmWindowTimeAll 
		extends RichAllWindowFunction<
					Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String>, 
					Iterator<Tuple2<String, Integer>>, TimeWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3984649183085126421L;

	@Override
	public void apply(TimeWindow window,
			Iterable<Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String>> input,
			Collector<Iterator<Tuple2<String, Integer>>> out)
					throws Exception {
		Iterator<Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String>> elems = input.iterator();
		
		Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String> ele;
		
    	System.out.println("=====================================");
        while(elems.hasNext()) {
        	ele = elems.next();
        	
        	System.out.println(ele);
        }
    	System.out.println("=====================================");
        
       	//out.collect(groupBy.values().iterator());
	}

}
