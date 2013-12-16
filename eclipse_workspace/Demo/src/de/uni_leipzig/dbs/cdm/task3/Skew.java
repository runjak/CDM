package de.uni_leipzig.dbs.cdm.task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import de.uni_leipzig.dbs.cdm.submission.MapReduceJobDriver;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * 1 Reducer: 25756212
 * */
public class Skew extends MapReduceJobDriver{
	@Override
	protected void configureMapReduceJob(JobConf job) {
		job.setInputFormat(TextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(SkewMapper.class);
		
	    job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyComparatorClass(Text.Comparator.class);
        job.setOutputValueGroupingComparator(Text.Comparator.class);

        job.setReducerClass(SkewReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormat(TextOutputFormat.class);
	}
}