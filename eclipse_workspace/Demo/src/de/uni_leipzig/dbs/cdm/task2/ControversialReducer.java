package de.uni_leipzig.dbs.cdm.task2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import de.uni_leipzig.dbs.cdm.io.TextIntPair;

/**
 * @author Jakob Runge
 * @since 2013-12-15
 * */
public class ControversialReducer extends MapReduceBase implements Reducer<IntWritable,TextIntPair,NullWritable,Text> {
	/**
	 * Since we don't use the output of this any further,
	 * we use a NullWritable as outputKey.
	 * */
	private NullWritable outputKey = NullWritable.get();
	private Text outputValue = new Text();
	/**
	 * How many min and max Ratings are necessary for a movie to be controversial.
	 * */
	private static int n = 100;
	
	@Override
	public void reduce(IntWritable key, Iterator<TextIntPair> values,
			OutputCollector<NullWritable, Text> collector, Reporter reporter)
			throws IOException {
		Text title = null;
		int maxRatings = 0, minRatings = 0;
		while(values.hasNext()){
			TextIntPair v = values.next();
			if(v.getFirst() != null){
				title = v.getFirst();
			}else if(v.getSecond() == 1){
				minRatings++;
			}else if(v.getSecond() == 5)
				maxRatings++;
		}
		//We only collect if we've got a title and min-/maxRatings satisfy n.
		if(title != null && maxRatings >= n && minRatings >= n){
			outputValue.set("("+title+","+minRatings+","+maxRatings+")");
			collector.collect(outputKey, outputValue);
		}
	}
}
