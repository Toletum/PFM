package org.toletum.pfm.streaming;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.toletum.pfm.Config;
import org.apache.flink.configuration.Configuration;



public class SinkFunctionCrimeStadistics 
	extends RichSinkFunction<TupleCrimeStreaming> {
	
	private Connection con;
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;
	
	static void CleanDB() {
		try {
			Class.forName(Config.DatabaseDriver);
			
			Connection con = DriverManager.getConnection(Config.DatabaseServer);
			
			Statement stmt = con.createStatement();
			stmt.execute("MATCH (D: Dia)-[R]->(B: Barrio) SET R.numero = R.batchNumero, R.rtNumero=0");
			stmt.close();
			con.close();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
	}

	@Override
	public void invoke(TupleCrimeStreaming crime) throws Exception {
		/*
		System.out.print(crime.f2);
		System.out.print(" - ");
		System.out.print(crime.f3);
		System.out.print(" - ");
		System.out.print(crime.f4);
		System.out.print(" - ");
		System.out.print(crime.f5);
		System.out.print(" - ");
		System.out.print(crime.f6);
		System.out.println();*/


		try {
			PreparedStatement stmt;
	
			stmt = this.con.prepareStatement(
					"MATCH (M: Mes {id: {1}})-[MD]->(D: Dia {id: {2}})-[T {hora: {3}}]->(B: Barrio {id: {4}}) " 
				  + " SET T.rtNumero = T.rtNumero + {5}, T.numero = T.numero + {5}"
				   );
					
	
			stmt.setInt(1, crime.f2);
			stmt.setInt(2, crime.f4);
			stmt.setInt(3, crime.f3);
			stmt.setString(4, crime.f5);
			stmt.setInt(5, crime.f6);
			stmt.executeQuery();
		
			stmt = this.con.prepareStatement(
					"MATCH (M: Mes {id: {1}})-[MD]->(D: Dia {id: {2}})-[T {hora: {3}}]->(B: Barrio {id: {4}}) " 
				  + " WITH AVG(T.numero) as MEDIA, D MATCH (D)-[T {hora: {3}}]->() SET T.media = MEDIA"
				   );
	
			stmt.setInt(1, crime.f2);
			stmt.setInt(2, crime.f4);
			stmt.setInt(3, crime.f3);
			stmt.setString(4, crime.f5);
			stmt.executeQuery();
			
		} catch(SQLException e) {
			e.printStackTrace();
		}


	}

	
	@Override
	public void close() throws IOException {
		try {
			this.con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Configuration parameters) {
		try {
			Class.forName(Config.DatabaseDriver);
			this.con = DriverManager.getConnection(Config.DatabaseServer);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
