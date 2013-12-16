package de.uni_leipzig.dbs.cdm.task2;

import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import de.uni_leipzig.dbs.cdm.helper.Patterns;
import de.uni_leipzig.dbs.cdm.io.TextIntPair;

/**
 * @author Jakob Runge
 * @since 2013-12-15
 * */
public class ControversialMapper extends MapReduceBase implements Mapper<LongWritable,Text,IntWritable,TextIntPair>{	
	private boolean isMovieFile = false;
	private IntWritable movieId = new IntWritable();
	private TextIntPair titleAndRating = new TextIntPair();
	
	@Override
	public void configure(JobConf job)
	{
		super.configure(job);
		this.isMovieFile = job.get("map.input.file").contains("movies.dat");
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, TextIntPair> collector, Reporter reporter)
			throws IOException {
		if(isMovieFile){
			Matcher movieMatcher = Patterns.moviePattern.matcher(value.toString());
			if(movieMatcher.matches()){
				this.movieId.set(Integer.parseInt(movieMatcher.group(1)));
				this.titleAndRating.setFirst(movieMatcher.group(2));
				collector.collect(this.movieId, this.titleAndRating);
			}
		}else{
			Matcher ratingMatcher = Patterns.ratingPattern.matcher(value.toString());
			if(ratingMatcher.matches()){
				this.movieId.set(Integer.parseInt(ratingMatcher.group(2)));
				if(ratingMatcher.group(3).equals("5") || ratingMatcher.group(3).equals("1")){
					this.titleAndRating.setSecond(Integer.parseInt(ratingMatcher.group(3)));
					collector.collect(this.movieId, this.titleAndRating);
				}
			}	
		}
	}
}