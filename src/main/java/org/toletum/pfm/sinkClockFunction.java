package org.toletum.pfm;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import redis.clients.jedis.Jedis;

public class sinkClockFunction 
implements SinkFunction<String> {
    private Jedis jedis;
	
	public sinkClockFunction() {
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;

	@Override
	public void invoke(String fechaHora) throws Exception {
		System.out.println(fechaHora);
    	jedis = new Jedis(Config.RedisServer);
		
    	jedis.set(Config.RedisClock, fechaHora);
    	
    	jedis.close();
	}
}
