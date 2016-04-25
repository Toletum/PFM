package org.toletum.pfm.streaming;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class GroupByWindowTime 
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
		
		HashMap<String, Tuple2<String,Integer>> groupBy = new HashMap<>(); 
		
		Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String> ele;
		
        while(elems.hasNext()) {
        	ele = elems.next();
        	
        	if(groupBy.containsKey(ele.f5)) {
        		groupBy.get(ele.f5).f1+=ele.f6;
        	} else {
	        	groupBy.put(ele.f5, new Tuple2<String, Integer>(ele.f5, ele.f6));
        	}
        	
        }
        
       	out.collect(groupBy.values().iterator());
	}

}
