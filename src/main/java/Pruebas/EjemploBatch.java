package Pruebas;

import java.io.IOException;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;


public class EjemploBatch {
	public class Salida extends RichOutputFormat<String> {
		private static final long serialVersionUID = 6119956316015038988L;
		private int taskNumber, numTasks;
		@Override
		public void configure(Configuration parameters) {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void open(int taskNumber, int numTasks) throws IOException {
			this.taskNumber=taskNumber;
			this.numTasks=numTasks;
		}
		@Override
		public void writeRecord(String record) throws IOException {
			System.out.println("writeRecord");
			
			System.out.print(this.taskNumber);
			System.out.print("/");
			System.out.print(this.numTasks);
			System.out.print(" - ");
			System.out.print(record);
		}
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	public EjemploBatch(ExecutionEnvironment env ) throws Exception {
		
		final String CSV = "/home/toletum/qui.txt";
		
		DataSet<String> fichero = env.readTextFile(CSV);
		
		fichero
		.print();
		
	}

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		new EjemploBatch(env);
		
	}
}
