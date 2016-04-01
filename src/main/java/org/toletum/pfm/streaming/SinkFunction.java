package org.toletum.pfm.streaming;

import java.io.IOException;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;
import org.apache.flink.configuration.Configuration;


import redis.clients.jedis.Jedis;

public class SinkFunction 
	extends RichSinkFunction<Tuple7<String, String, String, String, String, String,String>> {
	
    private Jedis jedis;
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;

	@Override
	public void invoke(Tuple7<String, String, String, String, String, String, String> crime) throws Exception {
		jedis.lpush(Config.RedisCrimes, crime.f0);
		jedis.ltrim(Config.RedisCrimes, 0, Config.RedisCrimesSize);
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
