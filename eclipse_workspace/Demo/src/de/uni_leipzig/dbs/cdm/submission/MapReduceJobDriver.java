/**
* @author Lars Kolb
* @since 26.07.2012
*/

package de.uni_leipzig.dbs.cdm.submission;


import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;


@SuppressWarnings("deprecation")
public abstract class MapReduceJobDriver
{
	public JobConf createJobConf(Map<String,String> parameters) throws IOException
	{
		JobConf job= new JobConf(new Configuration(true));
		
		for(Entry<String,String> entry : parameters.entrySet())
		{
			if(entry.getValue()!=null && !entry.getValue().isEmpty())
				job.set(entry.getKey(), entry.getValue());
		}
		
		configureMapReduceJob(job);
		return job;
	}
	
	protected abstract void configureMapReduceJob(JobConf job);
}