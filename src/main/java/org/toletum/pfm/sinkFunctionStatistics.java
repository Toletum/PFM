package org.toletum.pfm;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class sinkFunctionStatistics 
	extends RichSinkFunction<Tuple7<String, String, String, String, String, String,String>> {
	
	private transient Connection con=null;
	private PreparedStatement stmt;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 2859601213304525959L;
	
	public static void CleanDB() {
		try {
			Class.forName("org.neo4j.jdbc.Driver");
			Connection con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
			
			Statement st = con.createStatement();
			st.execute("MATCH (B: Barrio) SET B.delitos = B.batchDelitos, B.realTimeDelitos = 0");
			st.close();
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
	
	@Override
	public void invoke(Tuple7<String, String, String, String, String, String, String> crime) throws Exception {
		Batch.log("invoke");
		
		Integer month = Utils.getMonth2(crime.f2);
		Integer dayOfWeek = Utils.getDayOfWeek2(crime.f2);
		Integer minutes = Utils.getMinutes(crime.f3);
		String Barrio = crime.f4;
		Integer Num = Utils.getNum(crime.f6);
		
		this.stmt = this.con.prepareStatement("MATCH (R: Root)-->"
				+ "(M: Mes { id: {1}})-->"
				+ "(D: Dia { id: {2}})-->"
				+ "(H: Hora { id: {3}})-->"
				+ "(B: Barrio { id: {4}})"
				+ " SET B.realTimeDelitos = B.realTimeDelitos + {5},"
				+ "     B.delitos = B.delitos + {5} "
		          );
				
				
		stmt.setInt(1, month);
		stmt.setInt(2, dayOfWeek);
		stmt.setInt(3, minutes);
		stmt.setString(4, Barrio);
		stmt.setInt(5, Num);
		stmt.executeQuery();
		stmt.executeQuery();
		this.stmt.close();
		
		//ML... jejeje
		
		this.stmt = this.con.prepareStatement("MATCH (R: Root)-->"
				+ "(M: Mes { id: {1}})-->"
				+ "(D: Dia { id: {2}})-->"
				+ "(H: Hora { id: {3}})-->"
				+ "(B: Barrio)"
				+ "WITH SUM(B.delitos) AS totalDelitos "
				+ "MATCH (R: Root)-->"
				+ "(M: Mes { id: {1}})-->"
				+ "(D: Dia { id: {2}})-->"
				+ "(H: Hora { id: {3}})-->"
				+ "(B: Barrio) "
				+ "SET B.probabilidad = (B.delitos*100.0)/totalDelitos"
				);		
		
		stmt.setInt(1, month);
		stmt.setInt(2, dayOfWeek);
		stmt.setInt(3, minutes);
		stmt.executeQuery();
		this.stmt.close();
		
	}

	
	@Override
	public void close() throws IOException {
		Batch.log("CLOSE JDBC");
		
		try {
			this.con.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void open(Configuration parameters) {
		Batch.log("OPEN JDBC");
		try {
			Class.forName("org.neo4j.jdbc.Driver");
			this.con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
