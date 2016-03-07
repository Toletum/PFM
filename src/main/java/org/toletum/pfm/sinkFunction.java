package org.toletum.pfm;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import redis.clients.jedis.Jedis;

public class sinkFunction 
implements SinkFunction<Tuple7<String, String, String, String, String, String,String>> {
    private Jedis jedis;
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;

	@Override
	public void invoke(Tuple7<String, String, String, String, String, String, String> crime) throws Exception {
		System.out.println(crime.f0);
    	jedis = new Jedis(Config.RedisServer);
		
		jedis.lpush(Config.RedisCrimes, crime.f0);
		jedis.ltrim(Config.RedisCrimes, 0, 9);
		
		jedis.close();
		
	}
}
