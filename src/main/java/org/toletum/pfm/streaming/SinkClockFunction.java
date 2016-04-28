package org.toletum.pfm.streaming;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;

public class SinkClockFunction 
extends RichSinkFunction<String> {
    private Jedis jedis;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;
	
	private String key;

	public SinkClockFunction(String key) {
		this.key = key;
	}
	
	@Override
	public void close() throws IOException {
		jedis.close();
	}
	
	@Override
	public void open(Configuration parameters) {
    	jedis = new Jedis(Config.RedisServer);
	}
	
	@Override
	public void invoke(String fechaHora) throws Exception {
    	jedis.set(key, fechaHora);
	}
}
