package com.nanda.hadoop.muxiaocao.mapreduce.sort.secondsort.matrix;

import java.util.HashMap;
import java.util.Set;


public class User {
	
	private String userID;
	private HashMap<Integer, Integer> history;
	// 欧氏距离
	private double userLength;
	// min itemID
	private int minItem;
	// max itemID
	private int maxItem;
	
	public User(String userID, HashMap<Integer, Integer> history) {
		super();
		this.userID = userID;
		this.history = history;
		initInfo(history);
	}
	private User() {

	}
	@Override
	public String toString() {
		return "User [userID=" + userID + ", history=" + history.size() + "]";
	}
	
	public int getMinItem() {
		return minItem;
	}
	public void setMinItem(int minItem) {
		this.minItem = minItem;
	}
	public int getMaxItem() {
		return maxItem;
	}
	public void setMaxItem(int maxItem) {
		this.maxItem = maxItem;
	}
	public void setUserLength(double userLength) {
		this.userLength = userLength;
	}
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public HashMap<Integer, Integer> getHistory() {
		return history;
	}
	/**
	 * 初始化参数，在user初始化时调用
	 * 初始化的参数包括：
	 * 	欧氏距离  最小最大itemID
	 * @param history
	 */
	public void initInfo(HashMap<Integer, Integer> history) {
		this.history = history;
		int sum = 0;
		int tmp = 0;
		int min = Integer.MAX_VALUE;
		int max = 0;
		Set<Integer> keySet = history.keySet();
		for (Integer integer : keySet) {

			if (integer < min) {
				min = integer;
			}
			if (integer > max) {
				max = integer;
			}
			tmp = history.get(integer);
			sum += tmp * tmp;
		}
		userLength = Math.sqrt(sum);
		minItem = min;
		maxItem = max;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((history == null) ? 0 : history.hashCode());
		result = prime * result + ((userID == null) ? 0 : userID.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		User other = (User) obj;
		if (history == null) {
			if (other.history != null)
				return false;
		} else if (!history.equals(other.history))
			return false;
		if (userID == null) {
			if (other.userID != null)
				return false;
		} else if (!userID.equals(other.userID))
			return false;
		return true;
	}
	
	/**
	 * 返回欧氏距离
	 * @return
	 */
	public double getUserLength() {
	
		return userLength;
	}
}
