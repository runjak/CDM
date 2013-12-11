package de.uni_leipzig.dbs.cdm.demo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DocFrequencyCombiner extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
{
	@Override
	public void configure(JobConf job)
	{
		super.configure(job);
	}
	
	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> collector, Reporter reporter) throws IOException
	{
		int freq= 0;
		while(values.hasNext())
			freq+= values.next().get();
	
		collector.collect(key, new IntWritable(freq));
	}
	
	@Override
	public void close()
	{
		
	}
}
