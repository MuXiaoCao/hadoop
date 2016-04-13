package com.nanda.hadoop.muxiaocao.mapreduce.ii;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PositionBean implements WritableComparable<PositionBean>{

	private String fileName;
	private String word;
	private long count;
	
	public PositionBean() {
		
	}

	public PositionBean(String fileName, String word,long count) {
		this.fileName = fileName;
		this.count = count;
		this.word = word;
	}


	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	@Override
	public String toString() {
		return "PositionBean [fileName=" + fileName + ", word=" + word
				+ ", count=" + count + "]";
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public int compareTo(PositionBean o) {
		return o.getFileName().compareTo(getFileName());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(fileName);
		out.writeLong(getCount());
		out.writeUTF(word);
	}

	public void readFields(DataInput in) throws IOException {
		fileName = in.readUTF();
		count = in.readLong();
		word= in.readUTF();
	}
	
}
