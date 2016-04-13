package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util;

import org.apache.hadoop.io.Text;

/**
 * 文本解析器处理工具抽象类
 * @author muxiaocao
 *
 */
public interface RecordParserInterface {
	/**
	 * 初始化文本解析器
	 * @param dataName
	 * 			字段名称
	 */
	public void init(Class<? extends DataInstanceInterface> cla,Class<?> interf);
	/**
	 * 解析text数据
	 * @param value
	 * 		文本数据
	 * @param
	 * 		分隔符
	 */
	public DataInstanceInterface parse(Text value,String split);
	
	/**
	 * 获取对应字段的value值
	 * @param dataName
	 * @return
	 */
	public String getDataValue(String dataName);

}
