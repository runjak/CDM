package de.uni_leipzig.dbs.cdm.task4;

import org.apache.hadoop.io.IntWritable;

/**
 * @author Jakob Runge
 * @since 2013-12-16
 * Build to enable sorting Count DESC
 * */
public class CountComparator extends IntWritable.Comparator{
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
		return super.compare(b1, s1, l1, b2, s2, l2) * -1;
	}
}