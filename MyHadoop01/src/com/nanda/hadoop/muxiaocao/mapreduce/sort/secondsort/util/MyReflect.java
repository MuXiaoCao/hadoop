package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util;

import java.lang.reflect.Field;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class MyReflect {

	public static Object myPropertyEditor(Field field,String context) {
		context = context.trim().replaceAll("-", "").replaceAll(" ", "");
		String type = field.getGenericType().getTypeName();
		if (type.equals(Text.class.getTypeName())) {
			return new Text(context);
		}else if(type.equals(LongWritable.class.getTypeName())) {
			return new LongWritable(Long.parseLong(context)%1000000);
		}else if (type.equals(Integer.class.getTypeName())) {
			return Integer.parseInt(context);
		}else if (type.equals(Long.class.getTypeName())) {
			return Long.parseLong(context);
		}else if (type.equals(IntWritable.class.getTypeName())) {
			return new IntWritable(Integer.parseInt(context));
		}else {
			return context;
		}
	}

	// 把一个字符串的第一个字母大写、效率是最高的方法
	public static String getSetMethod(String fildeName) throws Exception {
		byte[] items = fildeName.getBytes();
		if (items[0] < 'a') {
			return "set" + fildeName;
		}
		items[0] = (byte) ((char) items[0] - 'a' + 'A');
		return "set" + new String(items);
	}
}
