package org.toletum.pfm.batch;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple5;

public class Batch {
	final String CSV = "hdfs://hadoop:9000/BDP_History/Victim_Based_Crime.csv";
	
	ExecutionEnvironment env;
	
	DataSet<Tuple12<String,String,String,
    String,String,String,
    String,String,String,
    String,String,String>> inputCSV;
	
	DataSet<Tuple5<Integer, Integer, Integer, String, Integer>> DataCrimes;
	
	public static void log(String info) {
		System.out.println(info);
	}
	
	public void processCrime() throws Exception {
		
		FilterOperator<Tuple5<Integer, Integer, Integer, String, Integer>> DataCrimesF = this.DataCrimes
        .filter(new FilterFunctionAdapter());
        
		DataCrimesF.partitionByHash(0).distinct(0).output(new OutputFormatNode("meses.csv","Mes",0,0));
		DataCrimesF.partitionByHash(3).distinct(3).output(new OutputFormatNode("barrios.csv","Barrio",3,3));
		DataCrimesF.partitionByHash(0,2).distinct(0,2).output(new OutputFormatNode("dias.csv","Dia",2,0,2));
		DataCrimesF.partitionByHash(0,2).distinct(0,2).output(new OutputFormatMesesDias("mesesdias.csv"));

		DataCrimesF
        .groupBy(0,1,2,3)
        .sum(4)
        .output(new OutputFormatDelitos("delitos.csv"));
	}
	
	public Batch(ExecutionEnvironment env) throws Exception {
		this.env = env;
		
		this.inputCSV = env.readCsvFile(this.CSV)
						   .ignoreInvalidLines()
						   .fieldDelimiter(",")
						   .types(String.class, String.class, String.class, 
								  String.class, String.class, String.class, 
								  String.class, String.class, String.class, 
								  String.class, String.class, String.class);
	
		this.DataCrimes = this.inputCSV.map(new CrimeMap());
	}

	
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		Batch b = new Batch(env);
		
		b.processCrime();
		
		env.execute("PFM Batch Layer");

	}
	


}
