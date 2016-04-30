package org.toletum.pfm.streaming;

import java.util.HashMap;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class GroupByWindowCount 
		extends RichAllWindowFunction<TupleCrimeStreaming, 
					Iterator<Tuple2<String, Integer>>, GlobalWindow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3984649183085126421L;

	@Override
	public void apply(GlobalWindow window,
			Iterable<TupleCrimeStreaming> input,
			Collector<Iterator<Tuple2<String, Integer>>> out)
					throws Exception {
		
		Iterator<TupleCrimeStreaming> elems = input.iterator();
		
		HashMap<String, Tuple2<String,Integer>> groupBy = new HashMap<>(); 
		
		TupleCrimeStreaming ele;
		
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
