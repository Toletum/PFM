package org.toletum.pfm.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple9;

public class StreamingFilterFunction 
implements FilterFunction<Tuple9<Integer,String,Integer,Integer,
Integer,String,Integer,
String,String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6322140487099573422L;

	@Override
	public boolean filter(Tuple9<Integer,String,Integer,Integer,
			Integer,String,Integer,
			String,String> crime) throws Exception {
		return crime.f2!=null;
	}
}
