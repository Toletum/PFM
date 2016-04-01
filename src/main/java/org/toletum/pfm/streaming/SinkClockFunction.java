package org.toletum.pfm.streaming;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;

public class SinkClockFunction 
implements SinkFunction<String> {
    private Jedis jedis;
	
	public SinkClockFunction() {
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;

	@Override
	public void invoke(String fechaHora) throws Exception {
    	jedis = new Jedis(Config.RedisServer);
    	jedis.set(Config.RedisClock, fechaHora);
    	jedis.close();
	}
}
