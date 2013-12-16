package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Output pairs will be count, year.
 * We expect the count to be the first thing to map over.
 * */
public class CountYearMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,IntWritable>{
	public static int jahr = 1980;

	//It's notable, that the year is part of the title.
	private IntWritable count = new IntWritable();
	private IntWritable year = new IntWritable();
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, IntWritable> collector, Reporter reporter)
			throws IOException {
		//Parsing the title:
		Matcher titleMatcher = Patterns.moviePattern.matcher((CharSequence) value);
		if(titleMatcher.matches()){
			//Parsing the year
			Matcher yearMatcher = Patterns.yearPattern.matcher((CharSequence) titleMatcher.group(2));
			if(!yearMatcher.matches()) return; // Chk that we've got a year
			this.year.set(Integer.parseInt(yearMatcher.group(1)));
			if(this.year.get() < jahr) return; // Chk that the year is big enough.
			collector.collect(this.count, this.year);
		}else{ // If we've got no title, we expect this to be the count.
			this.count.set(Integer.parseInt(value.toString()));
		}
	}
}
