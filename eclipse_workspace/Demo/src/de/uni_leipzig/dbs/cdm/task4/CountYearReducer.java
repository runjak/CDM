package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class CountYearReducer extends MapReduceBase implements Reducer<IntWritable,IntWritable,NullWritable,Text>{
	private NullWritable outputKey = NullWritable.get();
	private Text outputValue = new Text();
	@Override
	public void reduce(IntWritable count, Iterator<IntWritable> years,
			OutputCollector<NullWritable, Text> collector, Reporter reporter)
			throws IOException {
		while(years.hasNext()){
			outputValue.set(years.next()+","+count);
			collector.collect(this.outputKey, this.outputValue);
		}
	}
}
