package org.toletum.pfm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;

public class StreamingCrimeSplitter 
	  implements MapFunction<String, Tuple7<String, String, String, String, String, String,String>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -9139495496732679596L;

		@Override
		public Tuple7<String, String, String, String, String, String, String> map(String value) throws Exception {
			String []values = value.split(";");
			
			Tuple7<String, String, String, String, String, String,String> t; 
			try {
				t = new Tuple7<String, String, String, String, String, String,String>(value, values[0],values[1],values[2],values[3],values[4],values[5]);
			} catch(Exception ex) {
				t = new Tuple7<String, String, String, String, String, String,String>(value, "ERROR","","","","","");
			}
			
			return t;
		}
}

