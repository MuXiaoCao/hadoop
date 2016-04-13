package com.nanda.hadoop.muxiaocao.RPC;



public class LoginServiceImpl implements LoginServiceInterface{

	public String login(String uname, String passwd) {
		
		return uname + " 登陆成功！！！ ";
	}
	
}
