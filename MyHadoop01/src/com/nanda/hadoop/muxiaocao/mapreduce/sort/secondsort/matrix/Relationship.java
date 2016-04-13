package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.matrix;

import java.util.HashMap;
import java.util.Set;

public class Relationship {
	private User user1;	// 行
	private User user2;	// 列
	private double relationship;
	
	
	public Relationship() {
	}

	/**
	 * 余弦相似度：
	 * 
	 * 		sum (i * i)
	 * -------------------------------------
	 * 	欧式距离 * 欧氏距离
	 * @return
	 */
	public double getRelationship() {
		
		return relationship;
	}
	
/*	public static void main(String[] args) {
		ArrayList<User> allUser = FileUtil.getAllUser();
		System.out.println(allUser.get(0).getUserID());
		System.out.println(allUser.get(0).getMaxItem());
		setUser(allUser.get(0),allUser.get(0));
	}*/
	
	public void setUser(User user1,User user2) {
		this.user1 = user1;
		this.user2 = user2;
		double sum = 0;
		HashMap<Integer, Integer> user1History = user1.getHistory();
		HashMap<Integer, Integer> user2History = user2.getHistory();
		Set<Integer> user1KeySet = user1History.keySet();
		Set<Integer> user2KeySet = user2History.keySet();
		end:for (Integer integer1: user1KeySet) {
			for (Integer integer2 : user2KeySet) {
				if (integer1 == integer2) {
					sum += user1History.get(integer1) * user2History.get(integer2);
				}
				if (integer1 < user2.getMinItem()) {
					break end;
				}
				if (integer1 > user2.getMaxItem()) {
					break end;
				}
			}
		}
		//System.out.println(sum / user1.getUserLength() / user2.getUserLength());
		relationship = sum / user1.getUserLength() / user2.getUserLength();
	}
	
	public User getUser1() {
		return user1;
	}
	public User getUser2() {
		return user2;
	}

}
