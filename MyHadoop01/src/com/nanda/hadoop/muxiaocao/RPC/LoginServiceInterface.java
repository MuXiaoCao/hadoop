package com.nanda.hadoop.muxiaocao.RPC;


public interface LoginServiceInterface {
	public static final long versionID = 1L;
	public String login(String uname,String passwd);
}
