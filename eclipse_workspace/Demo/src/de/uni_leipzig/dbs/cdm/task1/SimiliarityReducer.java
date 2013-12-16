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

import de.uni_leipzig.dbs.cdm.helper.Constants;
import de.uni_leipzig.dbs.cdm.helper.StringHelper;

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
			//Iterator<TextArrayWritable> iter = ValList.iterator();
			//Text[] tuples = findTuples(iter);
			Text[] tuples = findTuples(ValList);
			ValList.poll();
			for (Text t:tuples){
				output.collect(null, t);
			}
			tuples.d
			//iter = null;
		}

	}

	private Text[] findTuples(LinkedList<TextArrayWritable> inList){ //Iterator<TextArrayWritable> iter) {
		// TODO Auto-generated method stub
		ArrayList<Text> textList = new ArrayList<Text>();
		TextArrayWritable peer = new TextArrayWritable();
		Text[] outText;
		String bTitle, pTitle;
		Iterator<TextArrayWritable> iter = inList.iterator();
		TextArrayWritable base = iter.next();
		while (iter.hasNext()){
			peer = iter.next();
			bTitle = base.toStrings()[0];
			pTitle = peer.toStrings()[0];
			if (!(bTitle.equals(pTitle))){
			if (checkSim (base,peer)){
				String s = (bTitle + "," + pTitle);
				textList.add(new Text(s));
			}
			}
		}
		outText = textList.toArray(new Text[textList.size()]);
		return outText;
	}

	private boolean checkSim(TextArrayWritable base, TextArrayWritable peer) {
		// TODO Auto-generated method stub
		boolean sim = false;
		//Schnittmenge der Genres vergleichen
		String[] sBase = base.toStrings();
		String[] sPeer = peer.toStrings();
//		sim(m1,m2) = (2·|genres(m1) ∩ genres(m2)|)/(|genres(m1)|+|genres(m2)|)
		double s = ((2*(double)(StringHelper.numIntersect(sBase, sPeer)))/(double)(sBase.length-1 + sPeer.length-1));
		if (Constants.TEST) System.out.println(sBase[0] + ":" + sPeer[0] + "=" + s);
		if (s>Constants.SMIN) sim = true;
		return sim;
	}

}
