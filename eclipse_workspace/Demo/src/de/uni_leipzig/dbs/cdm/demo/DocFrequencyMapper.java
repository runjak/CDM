/**
 * @author Lars Kolb
 * @since 06.11.2013
 */


package de.uni_leipzig.dbs.cdm.demo;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class DocFrequencyMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
{
	private Text outputKey= new Text();
	private IntWritable ONE= new IntWritable(1);
	
	
	@Override
	public void configure(JobConf job)
	{
		super.configure(job);
	}
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text,IntWritable> collector, Reporter reporter) throws IOException
	{
		String[] words = value.toString().trim().toLowerCase().replaceAll("\\p{Punct}", "").replaceAll("\n", " ").replaceAll("\\p{Blank}+", " ").split(" ");
		for(String w : words){
			outputKey.set(w);
			collector.collect(outputKey, ONE);
		}
//		String term= value.toString().trim();
//		outputKey.set(term);
//		
//		collector.collect(outputKey, ONE);
	}
	
	@Override
	public void close()
	{
		
	}
}