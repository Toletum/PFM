package org.toletum.pfm.streaming;


import java.io.IOException;
import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;

public class SinkAlarmWindow 
	extends RichSinkFunction<Tuple2<String, Date>>{

    /**
	 * 
	 */
	private static final long serialVersionUID = 7881357089072872617L;
	
	private Jedis jedis;
    private String key;
    private Integer size;

    
	public SinkAlarmWindow(String key, Integer size) {
		this.key=key;
		this.size=size;
	}
    
	@Override
	public void invoke(Tuple2<String, Date> record) throws Exception {
		System.out.println(record.f0+";"+record.f1);

		jedis.lpush(this.key, record.f0+";"+record.f1);
		jedis.ltrim(this.key, 0, this.size);
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
