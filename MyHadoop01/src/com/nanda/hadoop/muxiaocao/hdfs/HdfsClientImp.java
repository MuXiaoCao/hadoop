package com.nanda.hadoop.muxiaocao.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class HdfsClientImp implements HdfsCilentInterface{

	private FileSystem fs;

	@Before
	public void getFs() {
		// 可设置参数（优先级比配置文件要高）
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://muxiaocao:9000/");
		conf.set("dfs.replication","1");
		// 获得文件系统对象
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 文件的上传
	@Test
	public void testUpLoad()  {

		// 创建hdfs路径
		Path path = new Path("hdfs://muxiaocao:9000/fromlocalFile.tgz");
		// 从文件系统中创建制定文件的输出流
		FSDataOutputStream os;
		try {
			os = fs.create(path);
			// 获得本地文件的输入流
			FileInputStream is = new FileInputStream(new File(
					"/home/muxiaocao/tools/003---JDK/jdk-8u60-linux-x64.tar.gz"));
			// 输出流 复制到输入流里，完成文件的上传
			IOUtils.copy(is, os);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//文件下载
	@Test
	public void testDownLoad() {
		// 创建hdfs下载路径
		Path path = new Path("hdfs://muxiaocao:9000/fromlocalFile.tgz");
		try {
			// 从文件系统中创建制定文件的输入流
			FSDataInputStream is = fs.open(path);
			// 获得本地文件的输出流
			FileOutputStream os = new FileOutputStream(new File("/home/muxiaocao/myFile/fromhdfsFile.tgz"));
			// 输出流 复制到输入流里面，完成文件的下载
			IOUtils.copy(is, os);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testDeleteFile() {
		try {
			fs.delete(new Path("hdfs://muxiaocao:9000/fromlocalFile.tgz"), true);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
