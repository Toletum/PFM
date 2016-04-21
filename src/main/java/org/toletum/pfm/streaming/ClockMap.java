package org.toletum.pfm.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.toletum.pfm.Utils;

public class ClockMap 
	implements MapFunction <String, 
	Tuple8<String, Integer, Integer, Integer, 
	String, Integer, Integer, Integer>>{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7223057137884574087L;
	
	private String nextValue;

	@Override
	public Tuple8<String, Integer, Integer, Integer,
	String, Integer, Integer, Integer> map(String value) throws Exception {
//		System.out.println(value);
		
		nextValue = Utils.addHour(value);
		
		return new Tuple8<String, Integer, Integer, Integer,
				String, Integer, Integer, Integer>(
				value,
				Utils.getMonth2(value),
				Utils.getDayOfWeek2(value),
				Utils.getMinutes(value.substring(11)),
				nextValue,
				Utils.getMonth2(nextValue),
				Utils.getDayOfWeek2(nextValue),
				Utils.getMinutes(nextValue.substring(11))
				);
	}
}
