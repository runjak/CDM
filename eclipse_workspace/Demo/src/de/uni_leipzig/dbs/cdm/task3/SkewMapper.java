package de.uni_leipzig.dbs.cdm.task3;

import java.io.IOException;
import java.util.LinkedList;
import java.util.regex.Matcher;

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
public class SkewMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
	private Text outputKey = null;
	private LinkedList<Text> outputValues = new LinkedList<Text>();
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> collector, Reporter reporter)
			throws IOException {
		Matcher movieMatcher = Patterns.moviePattern.matcher(value.toString());
		if(movieMatcher.matches()){
			this.outputKey = new Text(movieMatcher.group(2));
			for(Text outputValue : this.outputValues){
				collector.collect(outputKey, outputValue);
			}
			this.outputValues.add(outputKey);
		}
	}
}
