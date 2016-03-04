package org.toletum.pfm;


import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple5;

public class CrimeGroup
extends Tuple5<Integer, Integer, Integer, String, Integer>
implements Serializable
{

	public CrimeGroup(Integer mes, Integer minutes, Integer dayOfWeek, String Barrio, Integer num) {
		super(mes, minutes, dayOfWeek, Barrio, num);
	}

	
	public void writeObject(java.io.ObjectOutputStream out)
		     throws IOException {
	}
	
    public void readObject(java.io.ObjectInputStream in)
		     throws IOException, ClassNotFoundException {
    	
    }
	public void readObjectNoData()  throws ObjectStreamException {
		
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4322065280221608406L;

}
