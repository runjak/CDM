/**
 * 
 */
package de.uni_leipzig.dbs.cdm.helper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * @author hadoop
 *
 */
public final class StringHelper {
	public static ArrayList<Text> toArrayList(Text[] inStr){
		List<Text> med = Arrays.asList(inStr);
		ArrayList<Text> out = new ArrayList<Text>(med);
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
	/**
	 * Berechnet die Anzahl der Elemente einer Schnittmenge zweier uebergebener String-Array.
	 * Dabei ist jedes Element wie bei einer Menge nur EINMAL im Array.
	 * Das bedeutet, die maximale Schnittmenge ist die Elementanzahl des kleineren Array.
	 * @param a
	 * @param b
	 * @return
	 */
	public static int numIntersect(String[] a, String[] b){

		int out = 0;
		for (String s : a){
			for (String r : b){
				if (s.equals(r)){
					out++;
					break;
				}
			}
		}
		return out;
	}
}
