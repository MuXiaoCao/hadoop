package com.nanda.hadoop.muxiaocao.mapreduce.bs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator; 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory; 
/** 
 * 自定义分组策略 
 * 将组合将中第一个值相同的分在一组 
 * @author zengzhaozheng 
 */
public class DefinedGroupSort extends WritableComparator{ 
    private static final Logger logger = LoggerFactory.getLogger(DefinedGroupSort.class); 
    public DefinedGroupSort() { 
        super(ItemBeanSimple.class,true); 
    }
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable a,WritableComparable b) {
		
		ItemBeanSimple item1 = (ItemBeanSimple)a;
		ItemBeanSimple item2 = (ItemBeanSimple)b;
		long l = Long.parseLong(item1.getUserID().toString());
		long r = Long.parseLong(item2.getUserID().toString());
		return l == r ? 0 : (l < r ? -1 : 1);
	} 
    
    	/*@Override
    public int compare(WritableComparable a, WritableComparable b) { 
        CombinationKey ck1 = (CombinationKey)a; 
        CombinationKey ck2 = (CombinationKey)b; 
                compareTo(ck2.getFirstKey())+"-------"); 
        return ck1.getFirstKey(pareTo(ck2.getFirstKey()); 
    } */
}
