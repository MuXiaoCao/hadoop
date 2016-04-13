package com.nanda.hadoop.muxiaocao.RPC;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class MyRPCLoginService {
	public static void main(String[] args) throws Exception, IOException {
		Builder builder = new RPC.Builder(new Configuration());
		builder.setPort(10000);
		builder.setBindAddress("muxiaocao");
		builder.setInstance(new LoginServiceImpl());
		builder.setProtocol(LoginServiceInterface.class);
		Server server = builder.build();
		server.start();
	}
}
