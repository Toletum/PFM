package org.toletum.pfm.batch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;

public class OutputFormatAdapter  
extends RichOutputFormat<Tuple5<Integer, Integer, Integer, String, Integer>> {

	private PreparedStatement stmt;
	private Connection con;
	
	private int countOk=0;
	private int countError=0;
	
	private int taskNumber=0; 
	private int numTasks=0;
	/**
	 * 
	 */
	private static final long serialVersionUID = 5970761203533693806L;

	public static void CleanDB()  {
		try {
			Class.forName("org.neo4j.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			Connection con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
			
			Statement stmt = con.createStatement();
			stmt.execute("MATCH (n) DETACH DELETE n");
			stmt.execute("CREATE (Root:Root { name: 'Root' })");
			stmt.close();
			con.close();
		} catch(SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void statistics()  {
		try {
			Class.forName("org.neo4j.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		try {
			Connection con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
			
			Statement stmt = con.createStatement();
			stmt.execute("MATCH (n) DETACH DELETE n");
			stmt.execute("CREATE (Root:Root { name: 'Root' })");
			stmt.close();
			con.close();
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
		
		System.out.println("Servidor: "+this.taskNumber);
		System.out.println("Tarea: "+this.numTasks);
		System.out.println("\tOk: "+this.countOk);
		System.out.println("\tError: "+this.countError);
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		this.taskNumber=taskNumber;
		this.numTasks=numTasks;
		
		try {
			Class.forName("org.neo4j.jdbc.Driver");
			this.con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void writeRecord(Tuple5<Integer, Integer, Integer, String, Integer> record) throws IOException {

		try {
			stmt = this.con.prepareStatement("MATCH (R: Root) CREATE UNIQUE (R)-[:Tiene {name: 'Tiene'}]->(M:Mes { id: {1} })"
					);
			stmt.setInt(1, record.f0);
			stmt.executeQuery();

			stmt = con.prepareStatement("MATCH (R: Root)-->(M: Mes {id: {1}}) CREATE UNIQUE (M)"
					+ "-[:`El` {name: 'El'}]->(D:Dia { id: {2} })"
					);
			stmt.setInt(1, record.f0);
			stmt.setInt(2, record.f2);
			stmt.executeQuery();
			
			stmt = con.prepareStatement("MATCH (R: Root)-->(M: Mes {id: {1}})-->(D:Dia { id: {2} }) CREATE UNIQUE (D)"
					+ "-[:`A las` {name: 'A las'}]->(H:Hora { id: {3} })"
					);
			stmt.setInt(1, record.f0);
			stmt.setInt(2, record.f2);
			stmt.setInt(3, record.f1);
			stmt.executeQuery();
			
			stmt = con.prepareStatement("MATCH (R: Root)-->(M: Mes {id: {1}})-->(D:Dia { id: {2} })-->(H:Hora { id: {3} }) CREATE UNIQUE (H)"
					+ "-[:`Hubo en` {name: 'Hubo en'}]->(B:Barrio { id: {4}, delitos: {5} , batchDelitos: {6}, realTimeDelitos: 0 })"
					);
			stmt.setInt(1, record.f0);
			stmt.setInt(2, record.f2);
			stmt.setInt(3, record.f1);
			stmt.setString(4, record.f3);
			stmt.setInt(5, record.f4);
			stmt.setInt(6, record.f4);
			stmt.executeQuery();
			
			countOk++;
		} catch (SQLException e) {
			e.printStackTrace();
			countError++;
		}

	}


}
