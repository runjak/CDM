package de.uni_leipzig.dbs.cdm.task4;

import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class CountMapper extends MapReduceBase implements Mapper<LongWritable,Text,NullWritable,IntWritable>{
	private NullWritable outputKey = NullWritable.get();
	private IntWritable year = new IntWritable();
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<NullWritable, IntWritable> collector, Reporter reporter)
			throws IOException {
		//Parsing the title
		Matcher titleMatcher = CountYearMapper.titlePattern.matcher((CharSequence) value);
		if(!titleMatcher.matches()) return; // Chk that we've got a title
		//Parsing the year
		Matcher yearMatcher = CountYearMapper.yearPattern.matcher((CharSequence) titleMatcher.group(0));
		if(!yearMatcher.matches()) return; // Chk that we've got a year
		this.year.set(Integer.parseInt(yearMatcher.group(0)));
		if(this.year.get() < CountYearMapper.jahr) return; // Chk that the year is big enough.
		//Done :)
		collector.collect(outputKey, year);
	}

}
