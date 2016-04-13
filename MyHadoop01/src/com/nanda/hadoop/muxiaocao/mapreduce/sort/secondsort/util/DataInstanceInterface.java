package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.util;

import java.util.HashMap;


/**
 * 自定义数据对象接口，mr解析器解析对象必须实现该借口
 * @author muxiaocao
 *
 */
public interface DataInstanceInterface {

	/**
	 * 得到数据对象的字段名称
	 * @return
	 */
	public HashMap<String, Boolean> getDataName();
	
	
}
