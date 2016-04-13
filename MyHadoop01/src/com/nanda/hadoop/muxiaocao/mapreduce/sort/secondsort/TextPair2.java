package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util.DataInstanceInterface;

public class TextPair2 implements WritableComparable<TextPair2>,DataInstanceInterface{

	private Text userID;
	private Text itemID;
	private LongWritable time;
	private IntWritable behave;
	
	
	public TextPair2() {
		set(new Text(), new LongWritable(), new Text(), new IntWritable());
	}
	public void set(Text userID,LongWritable time,Text itemID,IntWritable behave) {
		this.userID = userID;
		this.time = time;
		this.itemID = itemID;
		this.behave = behave;
	}
	
	@Override
	public String toString() {
		return "TextPair2 [userID=" + userID + ", itemID=" + itemID + ", time="
				+ time + ", behave=" + behave + "]";
	}
	public Text getUserID() {
		return userID;
	}

	public void setUserID(Text userID) {
		this.userID = userID;
	}

	public Text getItemID() {
		return itemID;
	}

	public void setItemID(Text itemID) {
		this.itemID = itemID;
	}

	public LongWritable getTime() {
		return time;
	}

	public void setTime(LongWritable time) {
		this.time = time;
	}

	public IntWritable getBehave() {
		return behave;
	}

	public void setBehave(IntWritable behave) {
		this.behave = behave;
	}

	

	public void write(DataOutput out) throws IOException {
		userID.write(out);
		time.write(out);
		itemID.write(out);
		behave.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		userID.readFields(in);
		time.readFields(in);
		itemID.readFields(in);
		behave.readFields(in);
	}
	@Override
	public int hashCode() {
		return userID.hashCode() * 163 + time.hashCode();
	}
	@Override
	public boolean equals(Object o) {
		
		if (o instanceof TextPair2) {
			TextPair2 tp = (TextPair2)o;
			return userID.equals(tp.userID) && time.equals(tp.time);
		}
		return false;
	}
	
	
	public HashMap<String, Boolean> getDataName() {
		LinkedHashMap<String, Boolean> map = new LinkedHashMap<String, Boolean>();
		//map.put("userID", false);
		map.put("time", false);
		map.put("itemID", false);
		map.put("behave", false);
		return map;
	}
	public int compareTo(TextPair2 o) {
		int cmp = userID.compareTo(o.userID);
		if (cmp!=0) {
			return cmp;
		}
		return time.compareTo(o.time);
	}

	
}
