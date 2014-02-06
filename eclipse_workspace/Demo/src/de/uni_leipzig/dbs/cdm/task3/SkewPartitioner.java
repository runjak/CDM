package de.uni_leipzig.dbs.cdm.task3;

import java.util.regex.Matcher;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import de.uni_leipzig.dbs.cdm.helper.Patterns;

public class SkewPartitioner extends Partitioner<Text, Text> {
	public int getPartition(Text key, Text value, int nParts){
		Matcher movieMatcher = Patterns.moviePattern.matcher(key.toString());
		if(movieMatcher.matches()){
			int year = Integer.parseInt(movieMatcher.group(1));
			if(year <= 1993){
				return 0;
			}else if(year <= 1998){
				return 1 % nParts;
			}else if(year <= 2002){
				return 2 % nParts;
			}
		}
		return 3 % nParts;
	}
}
