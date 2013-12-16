package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * Simply counts the amount of values we've got.
 * */
public class CountReducer extends MapReduceBase implements Reducer<NullWritable,IntWritable,NullWritable,IntWritable>{
	private NullWritable outputKey = NullWritable.get();
	private IntWritable count = new IntWritable();
	@Override
	public void reduce(NullWritable key, Iterator<IntWritable> values,
			OutputCollector<NullWritable, IntWritable> collector, Reporter reporter)
			throws IOException {
		int c = 0;
		while(values.hasNext()){
			c++;
			values.next();
		}
		this.count.set(c);
		collector.collect(this.outputKey, this.count);
	}
}
