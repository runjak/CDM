package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import de.uni_leipzig.dbs.cdm.helper.Patterns;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class YearMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,IntWritable>{
	private static final int jahr = 1980;
	private IntWritable year = new IntWritable();
	private IntWritable one = new IntWritable(1);
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, IntWritable> collector, Reporter reporter)
			throws IOException {
		Matcher movieMatcher = Patterns.moviePattern.matcher(value.toString());
		if(movieMatcher.matches()){
			Matcher yearMatcher = Patterns.yearPattern.matcher(movieMatcher.group(2));
			if(yearMatcher.matches()){
				int i = Integer.parseInt(yearMatcher.group(1));
				if(i > jahr){
					this.year.set(i);
					collector.collect(this.year, this.one);	
				}
			}
		}
	}
}
