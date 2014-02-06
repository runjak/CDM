/**
 * @author Lars Kolb
 * @since 06.11.2013
 */


package de.uni_leipzig.dbs.cdm.demo;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class DocFrequencyReducer extends MapReduceBase implements Reducer<Text,IntWritable,NullWritable,Text>
{
	private NullWritable outputKey= NullWritable.get();
	private Text outputValue= new Text();
	
	@Override
	public void configure(JobConf job)
	{
		super.configure(job);
	}
	
	@Override
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<NullWritable,Text> collector, Reporter reporter) throws IOException
	{
		String term= key.toString();
		
		int freq= 0;
		while(values.hasNext())
			freq+= values.next().get();
	
		outputValue.set(term+"\t"+freq);
		collector.collect(outputKey, outputValue);
	}
	
	@Override
	public void close()
	{
		
	}
}