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

/**
 *   user_id
 item_id
 behavior_type 
 用户对商品的行为类型
 包括浏览、收藏、加购物车、购买，对应取值分别是1、2、3、4。
 user_geohash
 用户位置的空间标识，可以为空
 由经纬度通过保密的算法生成
item_category
商品分类标识
字段脱敏
time
行为时间
精确到小时级别
 * @author muxiaocao
 *
 */
public class TextPair implements WritableComparable<TextPair>,DataInstanceInterface{

	private Text userID;
	private LongWritable time;
	private Text itemID;
	private IntWritable behave;
	private Text user_geohash;
	private Text item_geohash;
	
	public TextPair() {
		set(new Text(), new LongWritable(), new Text(), new IntWritable());
	}
	public void set(Text userID,LongWritable time,Text itemID,IntWritable behave) {
		this.userID = userID;
		this.time = time;
		this.itemID = itemID;
		this.behave = behave;
	}
	
	public Text getUserID() {
		return userID;
	}


	public void setUserID(Text userID) {
		this.userID = userID;
	}


	public Text getUser_geohash() {
		return user_geohash;
	}
	public void setUser_geohash(Text user_geohash) {
		this.user_geohash = user_geohash;
	}
	public Text getItem_geohash() {
		return item_geohash;
	}
	public void setItem_geohash(Text item_geohash) {
		this.item_geohash = item_geohash;
	}
	


	public Text getItemID() {
		return itemID;
	}


	public void setItemID(Text itemID) {
		this.itemID = itemID;
	}


	public IntWritable getBehave() {
		return behave;
	}


	public LongWritable getTime() {
		return time;
	}
	public void setTime(LongWritable time) {
		this.time = time;
	}
	public void setBehave(IntWritable behave) {
		this.behave = behave;
	}


	public int compareTo(TextPair o) {
		int cmp = userID.compareTo(o.userID);
		if (cmp!=0) {
			return cmp;
		}
		return time.compareTo(o.time);
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
		
		if (o instanceof TextPair) {
			TextPair tp = (TextPair)o;
			return userID.equals(tp.userID) && time.equals(tp.time);
		}
		return false;
	}
	@Override
	public String toString() {
		return time.toString() +"," + itemID.toString() +","+ behave.toString() + ";";
	}
	
	public HashMap<String, Boolean> getDataName() {
		LinkedHashMap<String, Boolean> map = new LinkedHashMap<String, Boolean>();
		map.put("userID", false);
		map.put("itemID", false);
		map.put("behave", false);
		map.put("user_geohash",  true);
		map.put("item_geohash", true);
		map.put("time", false);
		return map;
	}

	
}
