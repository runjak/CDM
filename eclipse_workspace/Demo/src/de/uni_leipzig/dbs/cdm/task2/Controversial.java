package de.uni_leipzig.dbs.cdm.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import de.uni_leipzig.dbs.cdm.io.TextIntPair;
import de.uni_leipzig.dbs.cdm.submission.MapReduceJobDriver;

/**
 * @author Jakob Runge
 * @since 2013-12-15
 * */
public class Controversial extends MapReduceJobDriver {

	@Override
	protected void configureMapReduceJob(JobConf job) {
		job.setInputFormat(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TextIntPair.class);
		job.setMapperClass(ControversialMapper.class);

		job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyComparatorClass(IntWritable.Comparator.class);
        job.setOutputValueGroupingComparator(TextIntPair.Comparator.class);
//		job.setPartitionerClass(TextIntPair.FirstKeyPartitioner.class);
		
	    job.setReducerClass(ControversialReducer.class);
		job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormat(TextOutputFormat.class);
	}

}
