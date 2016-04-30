package org.toletum.pfm.batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple12;
import org.toletum.pfm.Utils;

public class CrimeMap implements MapFunction<Tuple12<String,String,String,
String,String,String,
String,String,String,
String,String,String>, 
TupleCrime> {
 
	/**
	 * 
	 */
	private static final long serialVersionUID = -283046145121773789L;
	
	public TupleCrime map(
			Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> arg0)
    {
		Integer Mes;
		Integer Minutes;
		Integer dayOfWeek;
		String Barrio;
		Integer Num;

		Mes = Utils.getMonth(arg0.f0);
		Minutes = Utils.getMinutes(arg0.f1);
		dayOfWeek = Utils.getDayOfWeek(arg0.f0);
		Barrio = arg0.f7;
		Num = Utils.getNum(arg0.f11);
		
		return new TupleCrime(Mes, Minutes, dayOfWeek, Barrio, Num);
	}


	
}
