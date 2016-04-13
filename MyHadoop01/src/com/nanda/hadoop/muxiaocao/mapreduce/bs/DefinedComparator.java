package com.nanda.hadoop.muxiaocao.mapreduce.bs;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 自定义二次排序策略
 * 
 * @author zengzhaoheng
 */
public class DefinedComparator extends WritableComparator {
	private static final Logger logger = LoggerFactory
			.getLogger(DefinedComparator.class);

	public DefinedComparator() {
		super(ItemBeanSimple.class, true);
	}

	
/**
 * CombinationKey c1 = (CombinationKey) combinationKeyOne; 
        CombinationKey c2 = (CombinationKey) CombinationKeyOther; 
        * 确保进行排序的数据在同一个区内，如果不在同一个区则按照组合键中第一个键排序
        * 另外，这个判断是可以调整最终输出的组合键第一个值的排序 
        * 下面这种比较对第一个字段的排序是升序的，如果想降序这将c1和c2倒过来（假设1） 
        if(!c1.getFirstKey().equals(c2.getFirstKey())){ 
            ("---------out DefinedComparator flag---------"); 
            return c1.getFirstKey(pareTo(c2.getFirstKey()); 
            } 
        else{//按照组合键的第二个键的升序排序，将c1和c2倒过来则是按照数字的降序排序(假设2) 
            ("---------out DefinedComparator flag---------"); 
            return c1.getSecondKey().get()-c2.getSecondKey().get();//0,负数,正数 
 */
	@Override
    public int compare(WritableComparable combinationKeyOne, 
            WritableComparable CombinationKeyOther) { 
		ItemBeanSimple itemBean1 = (ItemBeanSimple) combinationKeyOne; 
		ItemBeanSimple itemBean2 = (ItemBeanSimple) CombinationKeyOther;              
		System.out.println("-------------");
		System.out.println(itemBean1.getTime());
		System.out.println(itemBean2.getTime());
		System.out.println(itemBean1.hashCode());
		System.out.println(itemBean2.hashCode());
		int a = (int)(itemBean1.getTime().get() - itemBean2.getTime().get())%1000000*1000000 + (itemBean1.hashCode() - itemBean2.hashCode())%100000;
		System.out.println(a);
		return a;
        } 
    
}