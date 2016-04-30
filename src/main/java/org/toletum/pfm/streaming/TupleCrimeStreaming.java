package org.toletum.pfm.streaming;

import org.apache.flink.api.java.tuple.Tuple9;

public class TupleCrimeStreaming 
	extends Tuple9<Integer, String, Integer, Integer, Integer, String, Integer, String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1648021090502643956L;

	public TupleCrimeStreaming() {
		super();
	}
	
	public TupleCrimeStreaming(Integer arg0, String arg1, Integer arg2, Integer arg3, Integer arg4, String arg5, Integer arg6, String arg7, String arg8) {
		super(arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
	}
}
