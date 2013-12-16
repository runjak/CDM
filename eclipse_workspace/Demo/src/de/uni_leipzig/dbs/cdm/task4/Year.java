package de.uni_leipzig.dbs.cdm.task4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import de.uni_leipzig.dbs.cdm.submission.MapReduceJobDriver;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class Year extends MapReduceJobDriver{
	@Override
	protected void configureMapReduceJob(JobConf job) {
		job.setInputFormat(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(YearMapper.class);
		
	    job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyComparatorClass(IntWritable.Comparator.class);
      job.setOutputValueGroupingComparator(IntWritable.Comparator.class);
        
        job.setReducerClass(YearReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setOutputFormat(TextOutputFormat.class);
	}
}
