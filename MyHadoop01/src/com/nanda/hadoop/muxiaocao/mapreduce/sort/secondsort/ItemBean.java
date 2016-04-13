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

public class ItemBean implements WritableComparable<ItemBean>,DataInstanceInterface{
	
	private Text userID;
	private Text itemID;
	private IntWritable count;
	
	
	public ItemBean() {
		userID = new Text();
		itemID = new Text();
		count = new IntWritable();
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

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((itemID == null) ? 0 : itemID.hashCode());
		result = prime * result + ((userID == null) ? 0 : userID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemBean other = (ItemBean) obj;
		if (itemID == null) {
			if (other.itemID != null)
				return false;
		} else if (!itemID.equals(other.itemID))
			return false;
		if (userID == null) {
			if (other.userID != null)
				return false;
		} else if (!userID.equals(other.userID))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return itemID.toString() + "," + count.toString() + ";";
	}

	public int compareTo(ItemBean o) {
		
		int cmp = userID.compareTo(o.userID);
		if (cmp!=0) {
			return cmp;
		}
		return itemID.compareTo(o.itemID);
	}

	public void write(DataOutput out) throws IOException {
		//userID.write(out);
		itemID.write(out);
		count.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		itemID.readFields(in);
		count.readFields(in);;
	}

	public HashMap<String, Boolean> getDataName() {
		LinkedHashMap<String, Boolean> map = new LinkedHashMap<String, Boolean>();
		map.put("time", false);
		map.put("itemID", false);
		map.put("behave", false);
		return map;
	}
	
	
}
