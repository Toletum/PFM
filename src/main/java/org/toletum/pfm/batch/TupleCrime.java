package org.toletum.pfm.batch;

import org.apache.flink.api.java.tuple.Tuple5;

public class TupleCrime extends Tuple5<Integer, Integer, Integer, String, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2790728486441974496L;

	public TupleCrime() {
		super();
	}
	
	public TupleCrime(Integer arg0, Integer arg1, Integer arg2, String arg3, Integer arg4) {
		super(arg0, arg1, arg2, arg3, arg4);
	}
}
