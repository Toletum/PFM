package Pruebas;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Neo4JExample {
	private Connection con;
	
	public Neo4JExample() throws SQLException, ClassNotFoundException {
		Class.forName("org.neo4j.jdbc.Driver");
		con = DriverManager.getConnection("jdbc:neo4j://database:7474/");
	}
	
	public void CleanDB() throws SQLException {
	
		Statement stmt = con.createStatement();
		stmt.execute("MATCH (n) DETACH DELETE n");
		stmt.execute("CREATE (Root:Root { name: 'Root' })");
		stmt.close();
	}
	
	public void close() throws SQLException {
		con.close();
	}
	
	public void createExample() throws SQLException {
		PreparedStatement stmt = con.prepareStatement("MATCH (R: Root) CREATE (R)-[:TIENE {name: 'Tiene'}]->(M:Mes { id: {1}, name: {2}, contador: 0 })");
		
		String []meses = {"Enero", "Febrero", "Marzo", "Abril",
				"Mayo", "Junio", "Julio", "Agosto",
				"Septiembre", "Octubre", "Noviembre", "Diciembre"};
		
		for(int i=1;i<=meses.length;i++) {
			stmt.setInt(1, i);
			stmt.setString(2, meses[i-1]);
			stmt.executeQuery();
		}
		
		stmt.close();
		
	}
	
	public void queryExample() throws SQLException {
		Statement stmt = con.createStatement();
		PreparedStatement pre = con.prepareStatement("MATCH (M: Mes {id: {1} }) SET M.contador = M.contador + 1");
		int suma=0;
		
		ResultSet res = stmt.executeQuery("MATCH (R)-->(M: Mes) RETURN SUM(M.contador) AS TOTAL");
		if(res.next()) {
			suma=res.getInt("TOTAL");
		}
		res.close();
		
		res = stmt.executeQuery("MATCH (R)-[Rel]->(M: Mes) RETURN M.id, M.name, M.contador");
		
		while(res.next()) {
			System.out.print(res.getInt("M.id"));
			System.out.print(" ");
			System.out.print(res.getString("M.name"));
			System.out.print(" ");
			System.out.print(res.getInt("M.contador"));
			System.out.print(" ");
			System.out.print(suma);
			System.out.print(": ");
			System.out.print((res.getInt("M.contador")/(float)suma) * 100);

			System.out.println();
			//pre.setInt(1, res.getInt("M.id"));
			//pre.executeQuery();
			
		}
		
		pre.close();
		res.close();
		stmt.close();
	}


	public static void main(String[] args) throws SQLException, ClassNotFoundException {
		System.out.println("Neo4J Example...");
		
		Neo4JExample neo = new Neo4JExample();
		/*
		neo.CleanDB();
		neo.createExample();
		*/
		neo.queryExample();
		
		neo.close();
	}

}
