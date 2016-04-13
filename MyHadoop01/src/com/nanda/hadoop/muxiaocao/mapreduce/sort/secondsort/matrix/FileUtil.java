package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.matrix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import com.sun.corba.se.spi.ior.Writeable;

public class FileUtil {
	
	private static final String SRC_PATH = "/home/muxiaocao/sfd/user6/newuser61/part-r-00000";
	private static final String TAG_PATH = "/home/muxiaocao/sfd/user6/newuser61/matrix.txt";
	private static ArrayList<User> userList;
	@Test
	public static ArrayList<User> getAllUser() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(SRC_PATH)));
			String line = null;
			String[] datas;
			String[] items;
			ArrayList<User> list = new ArrayList<User>();
			while((line = br.readLine())!=null) {
				HashMap<Integer, Integer> history = new HashMap<Integer, Integer>();
				datas = StringUtils.split(line,"\t");
				String userID = datas[0];
				datas = StringUtils.split(datas[1],";");
				for (int i = 0; i < datas.length; i++) {
					items = StringUtils.split(datas[i],",");
					history.put(Integer.parseInt(items[0]), Integer.parseInt(items[1]));
				}
				User user = new User(userID, history);
				list.add(user);
			}
			br.close();
			userList = list;
			return list;
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return null;
	}
	
	public static void writeMatrix(Relationship[][] relationships) throws IOException {
		
		BufferedWriter bWriter = new BufferedWriter(new FileWriter(new File(TAG_PATH)));
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < userList.size(); i++) {
			sb.append(userList.get(i).getUserID() + " ");
		}
		bWriter.write(sb.toString() + "\r\n");
		sb.delete(0, sb.length()-1);
		for (int i = 0; i < relationships.length; i++) {
			for (int j = 0; j < relationships.length; j++) {
				sb.append(relationships[i][j].getRelationship() + "\t");
			}
			bWriter.write(sb.toString() + "\r\n");
			sb.delete(0, sb.length()-1);
		}
		bWriter.close();
	}
}
