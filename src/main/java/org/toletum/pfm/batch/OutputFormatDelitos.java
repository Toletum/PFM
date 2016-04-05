package org.toletum.pfm.batch;

import java.io.IOException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OutputFormatDelitos 
extends RichOutputFormat<Tuple5<Integer, Integer, Integer, String, Integer>> {
	private String fileName;
	
	private int taskNumber=0; 
	private int numTasks=0;

	private FSDataOutputStream outputStream;
	private Path src;
	private FileSystem hdfs;

	/**
	 * 
	 */
	private static final long serialVersionUID = -7305627059160655428L;

	public OutputFormatDelitos(String fileName) {
		this.fileName=fileName;
	}
	
	@Override
	public void close() throws IOException {
		this.outputStream.close();
	}

	@Override
	public void configure(Configuration arg0) {
		// TODO Auto-generated method stub
		
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
		String f;
		
		// 1_1,CENTRAL,DELITOS,0,11,11,0

		f=record.f0.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write('_');
		f=record.f2.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');

		f=record.f3;
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');
		
		f="DELITOS";
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');
		
		f=record.f1.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');
		
		f=record.f4.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');
		
		f=record.f4.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');
		
		f="0";
		this.outputStream.write(f.getBytes());
		
		this.outputStream.write('\n');
	}


}
