/**
 * 
 */
package de.uni_leipzig.dbs.cdm.task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author hadoop
 *
 */
public class SimiliarityReducer extends MapReduceBase implements
		Reducer<IntWritable, TextArrayWritable, NullWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterator<TextArrayWritable> values,
			OutputCollector<NullWritable, Text> output, Reporter reporter)
			throws IOException {
		// TODO Auto-generated method stub
		LinkedList<TextArrayWritable> ValList = new LinkedList<TextArrayWritable>();
		while (values.hasNext()){
			ValList.add(values.next());
		}
		while (ValList.size()>1){
			Iterator<TextArrayWritable> iter = ValList.iterator();
			Text[] tuples = findTuples(iter);
			ValList.poll();
		}

	}

	private Text[] findTuples(Iterator<TextArrayWritable> iter) {
		// TODO Auto-generated method stub
		ArrayList<Text> textList = new ArrayList<Text>();
		TextArrayWritable peer = new TextArrayWritable();
		Text[] outText;
		TextArrayWritable base = iter.next();
		while (iter.hasNext()){
			peer = iter.next(); 
			if (checkSim (base,peer)){
				String s = (base.toStrings()[0] + "," + peer.toStrings()[0]);
				textList.add(new Text(s));
			}
		}
		outText = textList.toArray(new Text[textList.size()]);
		return outText;
	}

	private boolean checkSim(TextArrayWritable base, TextArrayWritable peer) {
		// TODO Auto-generated method stub
		boolean sim = false;
		//Schnittmenge der Genres vergleichen
		
		return sim;
	}

}
