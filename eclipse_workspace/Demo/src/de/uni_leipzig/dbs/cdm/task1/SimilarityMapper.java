/**
 * 
 */
package de.uni_leipzig.dbs.cdm.task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Matcher;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import de.uni_leipzig.dbs.cdm.helper.Constants;
import de.uni_leipzig.dbs.cdm.helper.Patterns;
import de.uni_leipzig.dbs.cdm.helper.StringHelper;

/**
 * @author Martin
 * Mappt die Zeilen einer Eingabedatei zu: Jahr, Liste von Strings.
 * Dabei beinhaltet die Liste von Strings im ersten Eintrag den Titel, die Restlichen sind die Genres
 */
public class SimilarityMapper extends MapReduceBase implements
		Mapper<LongWritable,Text,IntWritable,TextArrayWritable> {

	@Override
	public void configure(JobConf job)
	{
		super.configure(job);
	}
	
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<IntWritable, TextArrayWritable> output,
			Reporter reporter) throws IOException {
		IntWritable outKey = new IntWritable();
		TextArrayWritable outValue;
		Matcher fullMatcher = Patterns.moviePattern.matcher(value.toString());
		if (fullMatcher.matches()){
			String gString = fullMatcher.group(3);
			String[] sGenres = gString.split("|");
			
			//Minimum-Filter für die Anzahl der Genres
			if (sGenres.length >= Constants.GMIN){
			
				//Genres zu TextArray umformen
				Text[] genres = StringHelper.ConvertToTextArray(sGenres);
				//TextArray zu ArrayList umformen
				ArrayList<Text>  gList = StringHelper.toArrayList(genres); 
				Matcher yearMatcher = Patterns.yearPattern.matcher(fullMatcher.group(2));
				//Form passt, Eintrag befuellen
				if (yearMatcher.matches()){
					Text title = new Text(yearMatcher.group(0));
					String year = yearMatcher.group(1);
					//Schluessel parsen und casten
					outKey = new IntWritable(Integer.parseInt(year));
					//Liste fuer Titel und Genres anlegen
					ArrayList<Text> outList = new ArrayList<Text>();
					outList.add(title);
					outList.addAll(gList);
					Text[] outArray = outList.toArray(new Text[outList.size()]);
					outValue = new TextArrayWritable(outArray);
					output.collect(outKey, outValue);
				}
			}
		}
			
			
		
		
	}

}
