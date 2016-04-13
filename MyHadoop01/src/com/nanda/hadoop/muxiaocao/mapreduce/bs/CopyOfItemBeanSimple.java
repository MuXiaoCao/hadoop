package com.nanda.hadoop.muxiaocao.mapreduce.bs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.WritableComparable;

@InterfaceAudience.Public
@InterfaceStability.Stable
public class CopyOfItemBeanSimple implements WritableComparable<CopyOfItemBeanSimple>{
	
	private String userID;
	private long time;
	private String itemID;
	private int behavior;
	
	public CopyOfItemBeanSimple() {
	}

	public CopyOfItemBeanSimple(String time, String itemID, int behavior,String userID) {
		super();
		this.time = Long.parseLong(time.trim());
		this.itemID = itemID.trim();
		this.behavior = behavior;
		this.userID = userID.trim();
	}

	

	public String getUserID() {
		return userID;
	}

	public void setUserID(String userID) {
		this.userID = userID;
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

	public void write(DataOutput out) throws IOException {
		out.writeLong(time);
		out.writeUTF(itemID);
		out.writeInt(behavior);
		out.writeUTF(userID);
	}

	public void readFields(DataInput in) throws IOException {
		time = in.readLong();
		itemID = in.readUTF();
		behavior = in.readInt();
		userID = in.readUTF();
	}
	
	/**
	 *   @Override
  public int compareTo(VIntWritable o) {
    int thisValue = this.value;
    int thatValue = o.value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }
	 */
	public int compareTo(CopyOfItemBeanSimple itemBean1) {
		System.out.println("**********");
		System.out.println(itemBean1.getTime());
		System.out.println(this.time);
		System.out.println(itemBean1.hashCode());
		System.out.println(this.hashCode());
		int a = (int)(itemBean1.getTime() - time)%1000000*1000000 + (itemBean1.hashCode() - hashCode())%100000;
		System.out.println(a);
		return a;
	}

	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return super.equals(obj);
	}

	/*public int compareTo(ItemBeanSimple itemBean) {
		if (itemBean.getUserID() == userID) {
			return (int)(itemBean.getTime() - time);
		}else {
			return (int)((itemBean.getTime() - time)%1000000L) + (itemBean.hashCode()  - hashCode())%100000*100000;
		}
		
	}*/
	
	
	/*public int compareTo(ItemBeanSimple itemBean) {
			return (int)(itemBean.getTime() - time);
	}

	public int compare(ItemBeanSimple itemBean1, ItemBeanSimple itemBean2) {

		return (int)(itemBean1.getTime() - itemBean2.getTime())%1000000*1000000 + (itemBean1.hashCode() - itemBean2.hashCode())%100000;
	}*/


}
