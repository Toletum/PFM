package org.toletum.pfm.batch;


import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple5;
import org.toletum.pfm.CrimeMap;

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
		this.DataCrimes
        .groupBy(0,1,2,3)
        .sum(4)
        .filter(new FilterFunctionAdapter())
        .output(new OutputFormatAdapter());
	}
	
	public Batch(ExecutionEnvironment env) throws Exception {
		this.env = env;
		
		OutputFormatAdapter.CleanDB();
		
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
