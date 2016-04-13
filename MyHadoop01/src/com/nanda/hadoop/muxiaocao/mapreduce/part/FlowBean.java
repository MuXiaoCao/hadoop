package com.nanda.hadoop.muxiaocao.mapreduce.part;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {

	private String phone;
	private long up_flow;
	private long down_flow;
	private long sum_flow;
	
	public FlowBean() { }

	public FlowBean(String phone, long up_flow, long down_flow) {
		super();
		this.phone = phone;
		this.up_flow = up_flow;
		this.down_flow = down_flow;
		this.sum_flow = up_flow + down_flow;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public long getUp_flow() {
		return up_flow;
	}

	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}

	public long getDown_flow() {
		return down_flow;
	}

	public void setDown_flow(long down_flow) {
		this.down_flow = down_flow;
	}

	public long getSum_flow() {
		return sum_flow;
	}

	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}
	
	@Override
	public String toString() {
		return "" + phone + "	" + up_flow + "	" + down_flow + "	" + sum_flow;
	}

	public int compareTo(FlowBean o) {
		
		return (int)(o.getSum_flow() - sum_flow);
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(phone);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
	}

	public void readFields(DataInput in) throws IOException {
		phone = in.readUTF();
		up_flow = in.readLong();
		down_flow = in.readLong();
		sum_flow = in.readLong();
	}
	
	

}
