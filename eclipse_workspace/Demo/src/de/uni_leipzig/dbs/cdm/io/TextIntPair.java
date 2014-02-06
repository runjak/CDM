/**
* @author Lars Kolb
* @since 01.07.2010
*/

package de.uni_leipzig.dbs.cdm.io;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;


public class TextIntPair implements WritableComparable<TextIntPair>
{
	static
	{
		WritableComparator.define(TextIntPair.class, new Comparator());
	}
	
	private Text first;
	private int second;

	public TextIntPair()
	{
		first= new Text();
	}
	
	public TextIntPair(String first, int second)
	{
		this();
		set(first, second);
	}
	
	public TextIntPair(Text first, int second)
	{
		this();
		set(first, second);
	}
	
	public void setFirst(String first)
	{
		this.first.set(first);
	}
	
	public void setSecond(int second)
	{
		this.second= second;
	}
	
	public void setFirst(Text first)
	{
		this.first.set(first);
	}

	public void set(String first, int second)
	{
		setFirst(first);
		setSecond(second);
	}
	
	public void set(Text first, int second)
	{
		setFirst(first);
		setSecond(second);
	}
	
	public Text getFirst()
	{
		return first;
	}
	
	public int getSecond()
	{
		return second;
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		first.write(out);
		WritableUtils.writeVInt(out, second);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException
	{
		first.readFields(in);
		second= WritableUtils.readVInt(in);
	}
	
	@Override
	public String toString()
	{
		return first + "." + second;
	}
	
	@Override
	public int hashCode()
	{
		return first.hashCode() * 163 + second;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if(o == null)
			return false;
		if(this == o)
			return true;
		if(!(o instanceof TextIntPair))
			return false;
		
		TextIntPair t = (TextIntPair) o;
		return first.equals(t.first) && second==t.second;
	}
	
	@Override
	public int compareTo(TextIntPair tp)
	{
		int cmp= first.compareTo(tp.first);
		if(cmp != 0)
			return cmp;
		
		return second-tp.second;
	}
	
	
	public static class Comparator extends WritableComparator
	{
		public Comparator()
		{
			super(TextIntPair.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				int sizeOfLengthField1= WritableUtils.decodeVIntSize(b1[s1]);
				int sizeOfLengthField2= WritableUtils.decodeVIntSize(b2[s2]);
				int length1= readVInt(b1, s1);
				int length2= readVInt(b2, s2);
				s1+= sizeOfLengthField1;
				s2+= sizeOfLengthField2;
				int cmp= compareBytes(b1, s1, length1, b2, s2, length2);
				if(cmp!=0)
					return cmp;
				
				s1+= length1;
				s2+= length2;
				int thisValue= readVInt(b1, s1);
			    int thatValue= readVInt(b2, s2);
			    return thisValue<thatValue ? -1 : (thisValue==thatValue ? 0 : 1);
			}
			catch(IOException e)
			{
				throw new IllegalArgumentException(e);
			}
		}
	}
	
	
	public static class FirstComparator extends WritableComparator
	{
		public FirstComparator()
		{
			super(TextIntPair.class);
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			try
			{
				int sizeOfLengthField1= WritableUtils.decodeVIntSize(b1[s1]);
				int sizeOfLengthField2= WritableUtils.decodeVIntSize(b2[s2]);
				int length1= readVInt(b1, s1);
				int length2= readVInt(b2, s2);
				s1+= sizeOfLengthField1;
				s2+= sizeOfLengthField2;
				return compareBytes(b1, s1, length1, b2, s2, length2);
			}
			catch (IOException e)
			{
				throw new IllegalArgumentException(e);
			}
		}
		
		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b)
		{
			if(a instanceof TextIntPair && b instanceof TextIntPair)
				return ((TextIntPair) a).first.compareTo(((TextIntPair) b).first);
			
			return super.compare(a, b);
		}
	}
	
	
	public static class FirstKeyPartitioner implements Partitioner<TextIntPair,Object>
	{
		@Override
		public void configure(JobConf job)
		{
			
		}
		
		@Override
		public int getPartition(TextIntPair key, Object value, int numPartitions)
		{
			return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}
}