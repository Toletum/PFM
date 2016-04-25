package org.toletum.pfm.streaming;

import java.io.IOException;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;

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
	public void invoke(Iterator<Tuple2<String, Integer>> lista) throws Exception {
		String dat="";
        while(lista.hasNext()) {
        	elem=lista.next();
			dat+=elem.f0+";"+elem.f1+"|";
        }
        jedis.set(this.key, dat);
        System.out.println(this.key+"-->"+dat);
        dat=null;
        elem=null;
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
