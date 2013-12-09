/**
 * @author Lars Kolb
 * @since 06.11.2013
 */


package de.uni_leipzig.dbs.cdm.demo;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import de.uni_leipzig.dbs.cdm.submission.MapReduceJobDriver;


public class DocFrequency extends MapReduceJobDriver
{
	@Override
	protected void configureMapReduceJob(JobConf job)
	{
		job.setInputFormat(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(DocFrequencyMapper.class);
		
		
	    job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyComparatorClass(Text.Comparator.class);
        job.setOutputValueGroupingComparator(Text.Comparator.class);
        
        
        job.setReducerClass(DocFrequencyReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    
	    job.setCombinerClass(DocFrequencyReducer.class);
	}
}