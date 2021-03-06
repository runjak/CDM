package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class YearReducer extends MapReduceBase implements Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
	private IntWritable count = new IntWritable();
	@Override
	public void reduce(IntWritable year, Iterator<IntWritable> values,
			OutputCollector<IntWritable, IntWritable> collector, Reporter reporter)
			throws IOException {
		int c = 0;
		while(values.hasNext()){
			c += values.next().get();
		}
		this.count.set(c);
		collector.collect(this.count, year);
	}
}
