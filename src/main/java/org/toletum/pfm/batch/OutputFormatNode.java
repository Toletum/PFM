package org.toletum.pfm.batch;

import java.io.IOException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class OutputFormatNode  
extends RichOutputFormat<Tuple5<Integer, Integer, Integer, String, Integer>> {

	
	private int countOk=0;
	private int countError=0;
	
	private int taskNumber=0; 
	private int numTasks=0;
	
	private int fieldsID[];
	private String fileName;
	private String node;
	private int fieldName;
	
	
	private FSDataOutputStream outputStream;
	private Path src;
	private FileSystem hdfs;
	/**
	 * 
	 */
	private static final long serialVersionUID = 5970761203533693806L;

	public OutputFormatNode(String fileName, String node, int fieldName, int... fieldsID) throws IOException {
		this.fieldsID=fieldsID;
		this.fileName=fileName;
		this.node=node;
		this.fieldName=fieldName;
	}
	
	@Override
	public void close() throws IOException {
		this.outputStream.close();
		
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

		org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
		conf.set("fs.default.name", "hdfs://hadoop:9000/");
		
		this.hdfs = FileSystem.get(conf);
		
		this.src = new Path("/"+this.fileName+"_"+this.taskNumber+"_"+this.numTasks);
		
		this.outputStream = this.hdfs.create(src);
	}

	@Override
	public void writeRecord(Tuple5<Integer, Integer, Integer, String, Integer> record) throws IOException {
		System.out.println(record);

		String f;
		
		this.outputStream.write(this.node.getBytes());
		this.outputStream.write(',');
		
		f=record.getField(this.fieldName).toString();
		this.outputStream.write(f.getBytes());

		this.outputStream.write(',');
		
		for(int i=0;i<this.fieldsID.length;i++) {
			if(i>0) this.outputStream.write('_');
			
			f=record.getField(this.fieldsID[i]).toString();
			this.outputStream.write(f.getBytes());
		}
		
		this.outputStream.write('\n');
	}


}
