package org.toletum.pfm.batch;

import java.io.IOException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class OutputFormatMesesDias 
extends RichOutputFormat<TupleCrime> {

	private String fileName;
	
	private int taskNumber=0; 
	private int numTasks=0;

	private FSDataOutputStream outputStream;
	private Path src;
	private FileSystem hdfs;
	/**
	 * 
	 */
	private static final long serialVersionUID = -2555839892309282756L;
	
	public OutputFormatMesesDias(String fileName) {
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
	public void writeRecord(TupleCrime record) throws IOException {
		String f;
		
		f=record.f0.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write(',');
			
		f=record.f0.toString();
		this.outputStream.write(f.getBytes());
		this.outputStream.write('_');
		f=record.f2.toString();
		this.outputStream.write(f.getBytes());
		
		this.outputStream.write(',');
		this.outputStream.write("TIENE".getBytes());
		
		this.outputStream.write('\n');
	}

}
