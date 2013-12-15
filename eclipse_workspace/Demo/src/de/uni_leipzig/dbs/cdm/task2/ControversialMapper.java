package de.uni_leipzig.dbs.cdm.task2;

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

import de.uni_leipzig.dbs.cdm.io.TextIntPair;

/**
 * @author Jakob Runge
 * @since 2013-12-15
 * */
public class ControversialMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,TextIntPair>{
	private static Pattern ratingPattern = Pattern.compile("^\\d+::(\\d+)::(\\d+)::\\d+$");
	private static Pattern moviePattern = Pattern.compile("^(\\d+)::(.+)::.+$");
	
	private IntWritable movieId = new IntWritable();
	private TextIntPair titleAndRating = new TextIntPair();
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, TextIntPair> collector, Reporter reporter)
			throws IOException {
		Matcher ratingMatcher = ratingPattern.matcher((CharSequence) value);
		Matcher movieMatcher = moviePattern.matcher((CharSequence) value);
		if(ratingMatcher.matches()){
			this.movieId.set(Integer.parseInt(ratingMatcher.group(0)));
			this.titleAndRating.setSecond(Integer.parseInt(ratingMatcher.group(1)));
			collector.collect(movieId, titleAndRating);
		}else if(movieMatcher.matches()){
			this.movieId.set(Integer.parseInt(movieMatcher.group(0)));
			this.titleAndRating.setFirst(movieMatcher.group(1));
			collector.collect(movieId, titleAndRating);
		}
	}
}