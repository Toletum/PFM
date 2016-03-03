package org.toletum.pfm;

import org.apache.flink.api.java.tuple.Tuple5;

public class CrimeGroup extends Tuple5<Integer, Integer, Integer, String, Integer> {

	public CrimeGroup(Integer mes, Integer minutes, Integer dayOfWeek, String Barrio, Integer num) {
		super(mes, minutes, dayOfWeek, Barrio, num);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -4322065280221608406L;

}
