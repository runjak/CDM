package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class CountMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,IntWritable>{
	private IntWritable count = new IntWritable();
	private IntWritable year = new IntWritable();
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, IntWritable> collector, Reporter reporter)
			throws IOException {
		String[] vals = value.toString().split("\t");
		if(vals.length == 2){
			this.count.set(Integer.parseInt(vals[0]));
			this.year.set(Integer.parseInt(vals[1]));
			collector.collect(this.count, this.year);
		}
	}
}