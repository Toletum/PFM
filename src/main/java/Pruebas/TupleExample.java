package Pruebas;

import org.apache.flink.api.java.tuple.Tuple5;

public class TupleExample extends Tuple5<Integer, Integer, Integer, Integer, Integer> {

	public TupleExample(Integer mes, Integer minutes, Integer dayOfWeek, Integer codPos, Integer num) {
		super(mes, minutes, dayOfWeek, codPos, num);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -4322065280221608407L;

}
