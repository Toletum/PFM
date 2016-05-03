package org.toletum.pfm.streaming;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class SinkFuture 
	extends RichSinkFunction<Tuple8<String, Integer, Integer, Integer,
	String, Integer, Integer, Integer>>{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1561366229219424284L;
	
	private Connection con;
    private Jedis jedis;
    private String key;
    private String keyNext;
    
    public SinkFuture(String key, String keyNext) {
    	this.key = key;
    	this.keyNext = keyNext;
    }
	
	private ResultSet getInfo(Integer mes, Integer dia, Integer hora) throws SQLException {
		
		PreparedStatement stmt = con.prepareStatement("MATCH (M: Mes {id: {1}})-[MD]->(D: Dia {id: {2}})-[T {hora: {3}}]->(B: Barrio) RETURN B.id, T.numero, T.media ORDER BY T.numero DESC");
		
		stmt.setInt(1, mes);
		stmt.setInt(2, dia);
		stmt.setInt(3, hora);
		
		return stmt.executeQuery();
	}

	@Override
	public void invoke(Tuple8<String, Integer, Integer, Integer,
			String, Integer, Integer, Integer> record)  {
		/*
		System.out.print(record.f0);
		System.out.print(" ");
		System.out.print(record.f1);
		System.out.print(" ");
		System.out.print(record.f2);
		System.out.print(" ");
		System.out.print(record.f3);
		System.out.println();
		*/
		
		String dat;
		Transaction trac;
		
		trac = jedis.multi();
		
		try {
			
			trac.del(this.key);
			trac.del(this.keyNext);
			
			
			ResultSet res = getInfo(record.f1, record.f2, record.f3);
			
			while(res.next()) {
				
				dat=res.getString("B.id")+";"+res.getInt("T.numero")+";"+res.getInt("T.media");
				
				trac.lpush(this.key, dat);
			}
			
			dat=null;
			res = null;
			
			res = getInfo(record.f5, record.f6, record.f7);
			
			while(res.next()) {
				dat=res.getString("B.id")+";"+res.getInt("T.numero")+";"+res.getInt("T.media");
				
				trac.lpush(this.keyNext, dat);
			}
			
			dat=null;
			res = null;
			
			trac.exec();
		} catch(SQLException e) {
			trac.discard();
			e.printStackTrace();
		}
		
		try {
			trac.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		try {
			this.con.close();
			this.jedis.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Configuration parameters) {
		try {
			Class.forName(Config.DatabaseDriver);
			this.con = DriverManager.getConnection(Config.DatabaseServer);
			
	    	this.jedis = new Jedis(Config.RedisServer);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
