package com.nanda.hadoop.muxiaocao.mapreduce.bs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class ItemBeanSimple implements WritableComparable<ItemBeanSimple>{
	
	private Text userID;
	private LongWritable time;
	private String itemID;
	private IntWritable behavior;
	
	public ItemBeanSimple() {
		userID = new Text();
		time = new LongWritable();
		itemID = new String();
		behavior = new IntWritable();
	}

	public ItemBeanSimple(LongWritable time, Text itemID, IntWritable behavior,Text userID) {
		super();
		this.time = time;
		this.itemID = itemID.toString();
		this.behavior = behavior;
		this.userID = userID;
	}

	

	

	@Override
	public String toString() {
		return time + ", " + itemID + ", "
				+ behavior + ";";
	}


	/**
	 * @return the userID
	 */
	public Text getUserID() {
		return userID;
	}

	/**
	 * @param userID the userID to set
	 */
	public void setUserID(Text userID) {
		this.userID = userID;
	}

	/**
	 * @return the time
	 */
	public LongWritable getTime() {
		return time;
	}

	/**
	 * @param time the time to set
	 */
	public void setTime(LongWritable time) {
		this.time = time;
	}

	

	/**
	 * @return the itemID
	 */
	public String getItemID() {
		return itemID;
	}

	/**
	 * @param itemID the itemID to set
	 */
	public void setItemID(String itemID) {
		this.itemID = itemID;
	}

	/**
	 * @return the behavior
	 */
	public IntWritable getBehavior() {
		return behavior;
	}

	/**
	 * @param behavior the behavior to set
	 */
	public void setBehavior(IntWritable behavior) {
		this.behavior = behavior;
	}

	public void write(DataOutput out) throws IOException {
		this.time.write(out);
		out.writeUTF(itemID);
		this.behavior.write(out);
		this.userID.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.time.readFields(in);
		itemID = in.readUTF();
		this.behavior.readFields(in);
		this.userID.readFields(in);
	}
	
	/**
	 *   @Override
  public int compareTo(VIntWritable o) {
    int thisValue = this.value;
    int thatValue = o.value;
    return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
  }
	 */
	
	public int compareTo(ItemBeanSimple itemBean1) {
		System.out.println("**********");
		System.out.println(itemBean1.getTime());
		System.out.println(this.time);
		System.out.println(itemBean1.hashCode());
		System.out.println(this.hashCode());
		int a = (int)(itemBean1.getTime().get() - this.time.get())%1000000*1000000 + (itemBean1.hashCode() - hashCode())%100000;
		System.out.println(a);
		return a;
	}

	@Override
    //The hashCode() method is used by the HashPartitioner (the default partitioner in MapReduce)
    public int hashCode()
    {
        return (int)( this.time.get())%1000000*1000000 + (super.hashCode())%100000;
    }
    @Override
    public boolean equals(Object right)
    {
        if (right == null)
            return false;
        if (this == right)
            return true;
        if (right instanceof ItemBeanSimple)
        {
            ItemBeanSimple r = (ItemBeanSimple) right;
            return r.userID == userID && r.time == time;
        }
        else
        {
            return false;
        }
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
