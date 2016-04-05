package org.toletum.pfm.streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.toletum.pfm.Utils;

public class CrimeMapStreaming 
	  implements MapFunction<String, 
	  Tuple8<String,Integer,Integer,
	         Integer,String,Integer,
	         String,String>
      > {

		/**
		 * 
		 */
		private static final long serialVersionUID = -9139495496732679596L;

		@Override
		public 	  Tuple8<String,Integer,Integer,
        Integer,String,Integer,
        String,String> map(String value) throws Exception {
			String []values = value.split(",");
			
			Tuple8<String,Integer,Integer,
			Integer,String,Integer,
			String,String> t;
			
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
				
				t = new Tuple8<String,Integer,Integer,
						Integer,String,Integer,
						String,String>(value, Mes, Minutes,
								       dayOfWeek, Barrio, Num,
								       Lat, Lng);
				
			} catch(Exception ex) {
				
				t = new Tuple8<String,Integer,Integer,
						Integer,String,Integer,
						String,String>(value,null,null,
								              null,null,null,
								              null,null);
			}
			
			return t;
		}
}

