package org.toletum.pfm.streaming;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class SinkWindowsGroup 
	extends RichSinkFunction<Iterator<Tuple2<String, Integer>>>{

    private Jedis jedis;
    private Tuple2<String, Integer> elem;
    private String key;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 388846950483761998L;
	
	public SinkWindowsGroup(String key) {
		this.key=key;
	}

	@Override
	public void invoke(Iterator<Tuple2<String, Integer>> lista) throws SQLException {
		
		Transaction trac;
		
		trac = jedis.multi();
		
		trac.del(this.key);
		
		String dat;
		while(lista.hasNext()) {
			elem=lista.next();
			dat=elem.f0+";"+elem.f1;
			
			trac.lpush(this.key, dat);
		}

		trac.exec();
		
		dat=null;
		elem=null;
        
		try {
			trac.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		jedis.close();
	}
	
	@Override
	public void open(Configuration parameters) {
    	jedis = new Jedis(Config.RedisServer);
	}
}
