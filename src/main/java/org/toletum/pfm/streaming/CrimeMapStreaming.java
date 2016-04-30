package org.toletum.pfm.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.toletum.pfm.Utils;

public class CrimeMapStreaming 
	  implements MapFunction<String, TupleCrimeStreaming> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -9139495496732679596L;
		
		private int contador=0;

		@Override
		public TupleCrimeStreaming map(String value) throws Exception {
			String []values = value.split(",");
			
			TupleCrimeStreaming t;
			
			contador++;
			
			try {
				
				Integer Mes;
				Integer Minutes;
				Integer dayOfWeek;
				String Barrio;
				Integer Num;
				String Lat;
				String Lng;

				Mes = Utils.getMonth(values[0]);
				Minutes = Utils.getMinutes(values[1]);
				dayOfWeek = Utils.getDayOfWeek(values[0]);
				Barrio = values[7];
				Num = Utils.getNum(values[11]);
				Lat = Utils.clearNumber(values[9]);
				Lng = Utils.clearNumber(values[10]);
				
				t = new TupleCrimeStreaming(new Integer(contador), value, Mes, Minutes,
								       dayOfWeek, Barrio, Num,
								       Lat, Lng);
				
			} catch(Exception ex) {
				
				t = new TupleCrimeStreaming(new Integer(contador), value, new Integer(-1), new Integer(-1),
									   new Integer(-1),"",new Integer(-1),
								       "","");
			}
			
			return t;
		}
}

