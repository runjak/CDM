/**
 * 
 */
package de.uni_leipzig.dbs.cdm.task1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.HashPartitioner;

import de.uni_leipzig.dbs.cdm.demo.DocFrequencyCombiner;
import de.uni_leipzig.dbs.cdm.demo.DocFrequencyMapper;
import de.uni_leipzig.dbs.cdm.demo.DocFrequencyReducer;
import de.uni_leipzig.dbs.cdm.submission.MapReduceJobDriver;

/**
 * @author hadoop
 *
 */
public class Similarity extends MapReduceJobDriver {

	

	/* (non-Javadoc)
	 * @see de.uni_leipzig.dbs.cdm.submission.MapReduceJobDriver#configureMapReduceJob(org.apache.hadoop.mapred.JobConf)
	 */
	@Override
	protected void configureMapReduceJob(JobConf job)
	{
		job.setInputFormat(TextInputFormat.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		job.setMapperClass(SimilarityMapper.class);
		
		
	    job.setPartitionerClass(HashPartitioner.class);
        job.setOutputKeyComparatorClass(IntWritable.Comparator.class);
        job.setOutputValueGroupingComparator(Text.Comparator.class);
        
        
        job.setReducerClass(SimiliarityReducer.class);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setOutputFormat(TextOutputFormat.class);
	    
	}

}
