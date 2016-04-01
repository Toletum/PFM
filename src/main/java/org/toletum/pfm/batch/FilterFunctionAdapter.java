package org.toletum.pfm.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class FilterFunctionAdapter 
       implements FilterFunction<Tuple5<Integer, Integer, Integer, String, Integer>> {
	public static final String Barrios[] = { "CENTRAL",
								       "EASTERN",
								       "NORTHEASTERN",
								       "NORTHERN",
								       "NORTHWESTERN",
								       "SOUTHEASTERN",
								       "SOUTHERN",
								       "SOUTHWESTERN",
								       "WESTERN"};
	private boolean Ok=false;

	/**
	 * 
	 */
	private static final long serialVersionUID = -6145658134544839073L;

	@Override
	public boolean filter(Tuple5<Integer, Integer, Integer, String, Integer> value) throws Exception {
		// TODO Auto-generated method stub
		Ok=false;
		for(String s: Barrios){
			if(s.equals(value.f3)) {
				Ok=true;
			}
		}
		
		if(value.f0.intValue()<0) Ok=false;
		if(value.f1.intValue()<0) Ok=false;
		if(value.f2.intValue()<0) Ok=false;
		if(value.f4.intValue()<0) Ok=false;
		
		return Ok;
	}
	
}
