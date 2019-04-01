package com.stone.weibo;

import java.io.IOException;

public class Weibo {
	
	
	public static void init() throws IOException {
		// 创建命名空间
		Util.createNamespace(Constant.NAMESPACE);
		
		// 创建内容表
		Util.createTable(Constant.CONTENT, 1, "info");
		
		// 创建用户关系表
		Util.createTable(Constant.RELATIONS, 1, "attends","fans");
		
		// 创建收件箱表（多版本）
		Util.createTable(Constant.INBOX, 2, "info");
		
	}
	
	public static void main(String[] args) throws IOException {
		// 测试
		//init();
		
		// 1001,1002 发布微博
		//Util.createData("1001", "1001发布001");
		//Util.createData("1002", "1002发布001");
		
		// 1001关注1002和1003
		//Util.addAttend("1001", "1002","1003");
		
		// 获取1001初始化页面信息
		//Util.getInit("1001");
		
		// 1003发布微博
		//Util.createData("1003", "1003发布001");
		//Util.createData("1003", "1003发布002");
		//Util.createData("1003", "1003发布003");
		//System.out.println("==========");
		
		// 获取1001初始化页面信息
		Util.getInit("1001");
		
		// 取关
		Util.delAttend("1001", "1002");
		
		Util.getInit("1001");
		
	}

}
