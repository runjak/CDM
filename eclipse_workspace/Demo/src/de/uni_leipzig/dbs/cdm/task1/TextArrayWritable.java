/**
 * 
 */
package de.uni_leipzig.dbs.cdm.task1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author hadoop
 *
 */
public class TextArrayWritable extends ArrayWritable {

	/**
	 * @param valueClass
	 */
	public TextArrayWritable() {
		super(Text.class);
	}

	/**
	 * @param valueClass
	 * @param values
	 */
	public TextArrayWritable(Writable[] values) {
		this();
		this.set(values);
//		super(Text.class, values);
		// TODO Auto-generated constructor stub
	}

}
