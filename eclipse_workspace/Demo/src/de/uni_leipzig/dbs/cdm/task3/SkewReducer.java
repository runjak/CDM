package de.uni_leipzig.dbs.cdm.task3;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import de.uni_leipzig.dbs.cdm.helper.Patterns;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * */
public class SkewReducer extends MapReduceBase implements Reducer<Text,Text,NullWritable,Text>{
	private NullWritable outputKey = NullWritable.get();
	private Text outputValue = new Text();
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<NullWritable, Text> collector, Reporter reporter)
			throws IOException {
		while(values.hasNext()){
			Text value = values.next();
			outputValue.set(key.toString() + "," + value.toString());
			collector.collect(this.outputKey, this.outputValue);
		}
	}
}