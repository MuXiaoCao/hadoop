package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util.DataInstanceInterface;

public class TextKey2 implements WritableComparable<TextKey2>,DataInstanceInterface{

	
	private Text userID;
	private Text itemID;
	
	public TextKey2() {
		set(new Text(), new Text());
	}
	public void set(Text userID,Text itemID) {
		this.userID = userID;
		this.itemID = itemID;

	}
	
	@Override
	public String toString() {
		return "TextKey2 [userID=" + userID + ", itemID=" + itemID + "]";
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

	
	public void write(DataOutput out) throws IOException {
		userID.write(out);

		itemID.write(out);
	
	}

	public void readFields(DataInput in) throws IOException {
		userID.readFields(in);

		itemID.readFields(in);

	}
	@Override
	public int hashCode() {
		return userID.hashCode() * 163 + itemID.hashCode();
	}
	@Override
	public boolean equals(Object o) {
		
		if (o instanceof TextKey2) {
			TextKey2 tp = (TextKey2)o;
			return userID.equals(tp.userID) && itemID.equals(tp.itemID);
		}
		return false;
	}
	
	
	public HashMap<String, Boolean> getDataName() {
		LinkedHashMap<String, Boolean> map = new LinkedHashMap<String, Boolean>();
		//map.put("userID", false);
		map.put("time", false);
		map.put("itemID", false);
		return map;
	}
	public int compareTo(TextKey2 o) {
		int cmp = userID.compareTo(o.userID);
		if (cmp!=0) {
			return cmp;
		}
		return itemID.compareTo(o.itemID);
	}


}
