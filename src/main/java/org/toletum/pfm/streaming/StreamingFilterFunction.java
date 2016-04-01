package org.toletum.pfm.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple7;

public class StreamingFilterFunction 
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
