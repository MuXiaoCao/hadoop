package com.nanda.hadoop.muxiaocao.mapreduce.bs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ItemBean implements WritableComparable<ItemBean>{
	
	private long time;
	private String itemID;
	private int behavior;
	private String category;
	
	public ItemBean() {
	}

	public ItemBean(String time, String itemID, int behavior, String category) {
		super();
		this.time = Long.parseLong(time.replaceAll("-", "").replaceAll(" ", "").replaceAll("	", "").trim());
		this.itemID = itemID;
		this.behavior = behavior;
		this.category = category;
	}

	

	@Override
	public String toString() {
		return time + ", " + itemID + ", "
				+ behavior + ";";
	}

	

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getItemID() {
		return itemID;
	}

	public void setItemID(String itemID) {
		this.itemID = itemID;
	}

	public int getBehavior() {
		return behavior;
	}

	public void setBehavior(int behavior) {
		this.behavior = behavior;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(time);
		out.writeUTF(category);
		out.writeUTF(itemID);
		out.writeInt(behavior);
	}

	public void readFields(DataInput in) throws IOException {
		time = in.readLong();
		category = in.readUTF();
		itemID = in.readUTF();
		behavior = in.readInt();
	}

	public int compareTo(ItemBean itemBean) {
		return (int)(itemBean.getTime() - time);
	}

}
