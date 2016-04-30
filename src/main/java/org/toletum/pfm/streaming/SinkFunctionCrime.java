package org.toletum.pfm.streaming;

import java.io.IOException;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;
import org.apache.flink.configuration.Configuration;


import redis.clients.jedis.Jedis;

public class SinkFunctionCrime 
	extends RichSinkFunction<TupleCrimeStreaming> {
	
    private Jedis jedis;
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;

	private String key;
	private Integer size;
	
	public SinkFunctionCrime(String key, Integer size) {
		this.key = key;
		this.size = size;
			
	}
	
	@Override
	public void invoke(TupleCrimeStreaming crime) throws Exception {
		jedis.lpush(this.key, crime.f0+";"+crime.f7+";"+crime.f8+";"+crime.f5);
		jedis.ltrim(this.key, 0, this.size);
		
		System.out.println(crime.f0+";"+crime.f7+";"+crime.f8+";"+crime.f5);
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
