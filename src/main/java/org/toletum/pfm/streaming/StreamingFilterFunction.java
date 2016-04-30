package org.toletum.pfm.streaming;

import org.apache.flink.api.common.functions.FilterFunction;

public class StreamingFilterFunction 
implements FilterFunction<TupleCrimeStreaming> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6322140487099573422L;

	@Override
	public boolean filter(TupleCrimeStreaming crime) throws Exception {
		return crime.f2!=null && crime.f2>0;
	}
}
