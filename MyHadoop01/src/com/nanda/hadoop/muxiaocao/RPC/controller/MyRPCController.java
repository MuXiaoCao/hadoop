package com.nanda.hadoop.muxiaocao.RPC.controller;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.nanda.hadoop.muxiaocao.RPC.LoginServiceInterface;


public class MyRPCController {
	public static void main(String[] args) throws IOException {
		
		LoginServiceInterface proxy = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress(
				"muxiaocao", 10000), new Configuration());
		System.out.println(proxy.login("muxiaocao", "123"));
	}
}
