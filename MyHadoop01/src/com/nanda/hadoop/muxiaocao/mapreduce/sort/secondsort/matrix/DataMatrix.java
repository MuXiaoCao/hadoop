package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.matrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

/**
 * 通过用户列表，得到每个用户的推荐用户列表
 *	1. 读取文件得到用户列表
 *	2. 两两用户做相似度比较
 *	3. 填充矩阵，得到相似度矩阵
 * @author muxiaocao
 *
 */
public class DataMatrix {
	public static Relationship[][] userMatrix;
	public static Relationship relationship;
	public static void main(String[] args) {
		ArrayList<User> allUser = FileUtil.getAllUser();
		System.out.println(allUser.size());
		ArrayList<Relationship> list = null;
		userMatrix = new Relationship[allUser.size()][];
		for (int i = 0; i < allUser.size(); i++) {
			for (int j = 0; j < allUser.size(); j++) {
				relationship = new Relationship();
				list = new ArrayList<Relationship>();
				relationship.setUser(allUser.get(i), allUser.get(j));
				if (relationship.getRelationship() != 0.0) {
					list.add(relationship);
				}
			}
			if (!list.isEmpty()) {
				System.out.println(list.toArray());
				userMatrix[i] = (Relationship[])(list.toArray());
			}else {
				System.out.println("null");
			}
		}
		try {
			FileUtil.writeMatrix(userMatrix);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
