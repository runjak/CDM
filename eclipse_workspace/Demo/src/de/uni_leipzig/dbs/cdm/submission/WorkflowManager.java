/**
* @author Lars Kolb
* @since 26.06.2012
*/

package de.uni_leipzig.dbs.cdm.submission;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;



@SuppressWarnings("deprecation")
public final class WorkflowManager
{
	private static Map<Integer,Map<String,String>> argsPerJob= new TreeMap<Integer,Map<String,String>>();
	private static final String PROGRAMM_USAGE= WorkflowManager.class.getSimpleName()+" [-Dparam=value] [-Djob0.param=value] driverClass0 [driverClass1 [...]]";
	
	
	public static void main(String args[]) throws Exception
	{
		parseArgsAndExecuteWorkflow(args);
	}
	
	@SuppressWarnings("unchecked")
	public static void parseArgsAndExecuteWorkflow(String[] args) throws Exception
	{
		String[] jobClasses= parseCLIArgs(args);
		List<Job> jobs= new ArrayList<Job>(jobClasses.length);
		
		Map<String,String> commonCLIProperties= argsPerJob.get(-1);
		commonCLIProperties= commonCLIProperties==null ? new HashMap<String,String>() : commonCLIProperties;
		
		for(int i=0; i<jobClasses.length; i++)
		{
			Class<MapReduceJobDriver> driverClass;
			try
			{
				driverClass= (Class<MapReduceJobDriver>) Class.forName(jobClasses[i]);
				MapReduceJobDriver driver= driverClass.newInstance();
				
				Map<String,String> jobProperties= new HashMap<String,String>();
				jobProperties.putAll(commonCLIProperties);
				Map<String,String> jobCLIProperties= argsPerJob.get(i);
				jobCLIProperties= jobCLIProperties==null ? new HashMap<String,String>() : jobCLIProperties;
				
				List<Integer> dependingJobs= new ArrayList<Integer>(1);
				if(i>0)
					dependingJobs.add(i-1);
				
				jobProperties.putAll(jobCLIProperties);
				
				
				JobConf job= driver.createJobConf(jobProperties); 
				job.setJarByClass(WorkflowManager.class);
				if(job.getJar()==null || job.getJar().isEmpty())
					throw new RuntimeException("Jar containing the "+WorkflowManager.class.getSimpleName()+" could not be found");
				
				MyJob myJob= new MyJob(job);
				String str= "Dependencies for job "+i+" ("+driverClass.getSimpleName()+"): ";
				for(int j=0; j<dependingJobs.size(); j++)
				{
					int dependingJob= dependingJobs.get(j);
					str+= dependingJob+", ";
					myJob.addDependingJob(jobs.get(dependingJob));
				}
				if(dependingJobs.size()>1)
				{
					str= str.substring(0, str.lastIndexOf(","));
					System.out.println(str);
				}
				
				jobs.add(myJob);
			}
			catch(Exception e)
			{
				System.err.println("Job creation for MapReduce program "+jobClasses[i]+" failed");
				throw(e);
			}
		}
		
		JobControl jobControl= new JobControl("");
		jobControl.addJobs(jobs);
		
		Date startTime = new Date();
		GregorianCalendar cal= new GregorianCalendar();
		cal.setTimeZone(TimeZone.getTimeZone("Europa/Berlin"));
		cal.setTime(startTime);
		System.out.println("\n\nWorkflow started: "+cal.getTime());
		new Thread(jobControl).start();
		while(!jobControl.allFinished())
		{
			try
			{
				if(jobControl.getRunningJobs().size()>0)
				{
					Job runningJob= jobControl.getRunningJobs().get(0); //assume sequential job flow
					JobID jobID= runningJob.getAssignedJobID();
					JobClient jobClient= runningJob.getJobClient();
					jobClient.monitorAndPrintJob(runningJob.getJobConf(), jobClient.getJob(jobID));
				}
			}
			catch(IndexOutOfBoundsException e) { }
			
			try
			{
				Thread.sleep(500);
			}
			catch (InterruptedException e) {}
		}
		
		jobControl.stop();
		
		List<Job> failedJobs= jobControl.getFailedJobs();
		for(Job failedJob : failedJobs)
			System.out.println("Job "+failedJob.getJobID()+" failed: "+failedJob.getMessage());

		Date endTime = new Date();
		System.out.println("Worfklow finished: "+ endTime);
		System.out.println(dateDifToString(startTime, endTime));
	}
	
	@SuppressWarnings("static-access")
	private static String[] parseCLIArgs(String[] args)
	{
		Option propertyOption = OptionBuilder.withArgName("[jobn.]property=value").hasArgs(2).withValueSeparator()
				.withDescription("Use value for given property. Properties prefixed with \"jobn.\" are set only for" +
				" the n-th job passed to the driver programm.").create("D");

		Options options = new Options();
		options.addOption(propertyOption);
	
		if(args.length==0)
		{
			new HelpFormatter().printHelp(PROGRAMM_USAGE, options);
			throw new RuntimeException("No arguments passed");
		}

		CommandLine line= null;
		Properties properties= null;
		CommandLineParser parser= new GnuParser();
		try
		{
			line= parser.parse(options, args);
			properties= line.getOptionProperties("D");
		}
		catch(ParseException exp)
		{
			System.err.println("Parsing failed.  Reason: " + exp.getMessage());
			new HelpFormatter().printHelp(PROGRAMM_USAGE, options);
			throw new RuntimeException("Parsing arguments failed");
		}
		
		for(Entry<Object,Object> entry : properties.entrySet())
		{
			String property= (String) entry.getKey();
			String value= (String) entry.getValue();
			
			if(!property.matches("^job\\d+\\..+"))
			{
				Map<String,String> jobsArgs= argsPerJob.get(-1);
				if(jobsArgs==null)
				{
					jobsArgs= new HashMap<String,String>();
					jobsArgs.put(property, value);
					argsPerJob.put(-1, jobsArgs);
				}
				else
				{
					jobsArgs.put(property, value);
				}
			}
			else
			{
				int jobIndex= new Integer(property.substring(0, property.indexOf(".")).substring(3));
				property= property.substring(property.indexOf(".") + 1);
				
				Map<String,String> jobsArgs= argsPerJob.get(jobIndex);
				if(jobsArgs==null)
				{
					jobsArgs= new HashMap<String,String>();
					jobsArgs.put(property, value);
					argsPerJob.put(jobIndex, jobsArgs);
				}
				else
				{
					jobsArgs.put(property, value);
				}
			}
		}
		
		for(Entry<Integer,Map<String,String>> entry : argsPerJob.entrySet())
		{
			String str = "cli args ";
			str+= entry.getKey()==-1 ? "[all jobs]" : "[job "+entry.getKey()+"]";
			str+= ": "+entry.getValue().toString();
			System.out.println(str);
		}
		
		return line.getArgs();
	}
	
	private static String dateDifToString(Date start, Date stop)
	{
		long diff= stop.getTime() - start.getTime();
		long diffDays = diff / (24 * 60 * 60 * 1000);
		long diffHours = diff/(60 * 60 * 1000) - diffDays*24;
		long diffMinutes = diff/(60 * 1000) - diffDays*24*60 - diffHours*60;
		long diffSeconds = diff/1000 - diffDays*24*60*60 - diffHours*60*60 - diffMinutes*60;
	    
	    return "Duration: "+diffDays+"d "+diffHours+"h "+diffMinutes+"m "+diffSeconds+"s";
	}
	
	private static class MyJob extends org.apache.hadoop.mapred.jobcontrol.Job
	{
		private JobConf jobConf;
		
		public MyJob(JobConf jobConf) throws IOException
		{
			super(jobConf);
			this.jobConf= jobConf;
		}
		
		public MyJob(JobConf jobConf, ArrayList<org.apache.hadoop.mapred.jobcontrol.Job> dependentJobs) throws IOException
		{
			super(jobConf, dependentJobs);
			this.jobConf= jobConf;
		}

		
		@Override
		protected synchronized void submit()
		{
			Path outputPath= new Path(jobConf.get("mapred.output.dir"));
			System.out.println("Deleting existing output path: "+outputPath);

			try
			{
				FileSystem hdfs = FileSystem.get(jobConf);
				if(hdfs.exists(outputPath))
				{
					hdfs.delete(outputPath, true);
					hdfs.close();
				}
			}
			catch(IOException e)
			{
				throw new RuntimeException(e);
			}
			
			super.submit();
		}
	}
}