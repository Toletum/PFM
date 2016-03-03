package Pruebas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;

public class MapExample {
	
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		List<Tuple3<Integer, Integer, Integer>> crimes = new ArrayList<Tuple3<Integer, Integer, Integer>>(); 
		TupleTypeInfo<Tuple3<Integer, Integer, Integer>> tupleTypeInfo = new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		crimes.add(new Tuple3<Integer, Integer, Integer>(1,2,3));
		crimes.add(new Tuple3<Integer, Integer, Integer>(1,2,3));
		crimes.add(new Tuple3<Integer, Integer, Integer>(2,2,3));
		crimes.add(new Tuple3<Integer, Integer, Integer>(3,2,3));
		crimes.add(new Tuple3<Integer, Integer, Integer>(-1,2,3));
		
		DataSet<Tuple3<Integer, Integer, Integer>> dataSetCrimes =  env.fromCollection(crimes, tupleTypeInfo);

        DataSet<Tuple4<Integer, Integer, Integer, Integer>> wordCounts = dataSetCrimes
            .map(new LineSplitter())
            .groupBy(0,1,2)
            .sum(3)
            .filter(new FilterFunctionAdapter())
            ;
        
        //wordCounts.output(new OutputFormatAdapter<Tuple4<Integer, Integer, Integer, Integer>>());
        //env.execute("Prueba");
        wordCounts.print();
    }
    
    public static class FilterFunctionAdapter implements FilterFunction<Tuple4<Integer, Integer, Integer, Integer>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 524826121963909591L;
		
		public FilterFunctionAdapter() {
		}


		@Override
		public boolean filter(Tuple4<Integer, Integer, Integer, Integer> arg0) throws Exception {
			
			return arg0.f0.intValue()<0;
		}
    	
    }
    
    public static class OutputFormatAdapter<T>  extends RichOutputFormat<T> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 5970761203533693806L;
		
		public OutputFormatAdapter() {
		}

		@Override
		public void close() throws IOException {
			System.out.println("close");
		}

		@Override
		public void configure(Configuration arg0) {
			System.out.println("configure");
			
		}

		@Override
		public void open(int arg0, int arg1) throws IOException {
			System.out.println("open");
		}

		@Override
		public void writeRecord(T arg0) throws IOException {
			System.out.println("writeRecord");
			System.out.println(arg0);
		}
	

    }

    public static class LineSplitter implements MapFunction<Tuple3<Integer, Integer, Integer>, Tuple4<Integer, Integer, Integer, Integer>> {
        /**
		 * 
		 */
		private static final long serialVersionUID = -4940994918312005648L;

		@Override
		public Tuple4<Integer, Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> crime) throws Exception {
			return new Tuple4<Integer, Integer, Integer, Integer>((Integer)crime.getField(0), (Integer)crime.getField(1), (Integer)crime.getField(2), new Integer(1));
		}

    }
}

