package com.nanda.hadoop.muxiaocao.mapreduce.part;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;


public class AreaPartitioner<KEY,VALUE> extends Partitioner<KEY, VALUE>{
	
	private static HashMap<String, Integer> areaMap = new HashMap<String, Integer>();
	static {
		areaMap.put("13", 1);
		areaMap.put("14", 2);
		areaMap.put("15", 3);
		areaMap.put("16", 4);
		areaMap.put("17", 5);
		areaMap.put("18", 6);
	}
	
	@Override
	public int getPartition(KEY key, VALUE value, int arg2) {
		
		// 根据key -- 手机号，得到省份，返回省份的编号
		int areaCode = areaMap.get(key.toString().substring(0, 2));
		if (areaMap.get(key.toString().substring(0, 2)) == null) {
			areaCode = 0;
		}
		return areaCode;
	}

}
