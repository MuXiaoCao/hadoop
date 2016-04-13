package com.nanda.hadoop.muxiaocao.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientAPI implements HdfsCilentInterface{
	
	private FileSystem fs;

	@Before
	public void getFs() {
		// 可设置参数（优先级比配置文件要高）
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://muxiaocao:9000/");
		conf.set("dfs.replication","1");
		try {
			// 获得文件系统对象
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testUpLoad() {
		try {
			//fs.copyFromLocalFile( new Path("/home/muxiaocao/myFile/data/user.csv"),new Path("hdfs://muxiaocao:9000/myFile/userdata.csv"));
			fs.copyFromLocalFile( new Path("/home/muxiaocao/sfd/user1.txt/part-r-00000"),new Path("hdfs://muxiaocao:9000/myFile/user.txt"));
			
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Test
	public void testDownLoad() {
		try {
			fs.copyToLocalFile(new Path("hdfs://muxiaocao:9000/fromlocalFile.tgz"), new Path("/home/muxiaocao/myFile/fromhdfsFile.tgz"));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}

	@Test
	public void testDeleteFile() {
		try {
			//fs.delete(new Path("hdfs://muxiaocao:9000/myFile/output2"), true);
			fs.delete(new Path("hdfs://muxiaocao:9000/myFile/user.csv"), true);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
}
