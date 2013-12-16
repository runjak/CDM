/**
 * 
 */
package de.uni_leipzig.dbs.cdm.helper;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.Text;

/**
 * @author hadoop
 *
 */
public final class StringHelper {
	public static ArrayList<Text> toArrayList(Text[] inStr){
		ArrayList<Text> out = (ArrayList<Text>) Arrays.asList(inStr);
		return out;
	}

	public static Text[] ConvertToTextArray(String[] sGenres) {
		// TODO Auto-generated method stub
		Text[] outText = new Text[sGenres.length];
		int i = 0;
		for (String s : sGenres){
			outText[i] = new Text(s);
			i++;
		}
		return outText;
	}
}
