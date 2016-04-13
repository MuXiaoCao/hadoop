package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util.DataInstanceInterface;

public class TextKey31 implements WritableComparable<TextKey31>,DataInstanceInterface{

	
	private Text itemID;
	private IntWritable count;
	
	public TextKey31() {
		set(new Text(), new IntWritable());
	}
	public void set(Text itemID,IntWritable count) {
		this.itemID = itemID;
		this.count = count;

	}
	
	@Override
	public String toString() {
		return itemID + "," + count;
	}
	

	public IntWritable getCount() {
		return count;
	}
	public void setCount(IntWritable count) {
		this.count = count;
	}
	public Text getItemID() {
		return itemID;
	}

	public void setItemID(Text itemID) {
		this.itemID = itemID;
	}

	
	public void write(DataOutput out) throws IOException {

		itemID.write(out);
		count.write(out);
	
	}

	public void readFields(DataInput in) throws IOException {

		itemID.readFields(in);
		count.readFields(in);

	}
	@Override
	public int hashCode() {
		return itemID.hashCode();
	}
	@Override
	public boolean equals(Object o) {
		
		if (o instanceof TextKey31) {
			TextKey31 tp = (TextKey31)o;
			if (itemID.equals(tp.getItemID())) {
		
				return true;
			}else {
				return false;
			}
		}
		return false;
	}
	
	
	public HashMap<String, Boolean> getDataName() {
		LinkedHashMap<String, Boolean> map = new LinkedHashMap<String, Boolean>();
		//map.put("userID", false);
		map.put("itemID", false);
		map.put("count", false);
		return map;
	}
	public int compareTo(TextKey31 o) {
		int cmp = itemID.compareTo(o.itemID);
		if (cmp!=0) {
			return cmp;
		}
		return count.compareTo(o.count);
	}


}

