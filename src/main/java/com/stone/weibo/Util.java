package com.stone.weibo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class Util {

	private static Configuration configuration = HBaseConfiguration.create();

	static {
		configuration.set("hbase.zookeeper.quorum", "172.30.60.62");
	}

	// 创建命名空间
	public static void createNamespace(String ns) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();

		// 创建namespace描述器和命名空间
		NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
		admin.createNamespace(namespaceDescriptor);

		// 关闭资源
		admin.close();
		connection.close();
	}

	// 创建表
	public static void createTable(String tableName, int versions, String... cfs) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();

		// 创建表描述器
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		// 循环添加列族
		for (String cf : cfs) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			hColumnDescriptor.setMaxVersions(versions);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}
		admin.createTable(hTableDescriptor);

		// 关闭资源
		admin.close();
		connection.close();
	}

	/**
	 * 发布微博 
	 * 1.更新微博内容表数据 
	 * 2.更新收件箱表数据，需要先获取当前操作人的fans，再去依次更新收件箱表数据
	 * @param uid
	 * @param content
	 * @throws IOException
	 */
	public static void createData(String uid, String content) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);

		// 获取操作的表对象
		Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
		Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
		Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));

		// 拼接rowKey
		long ts = System.currentTimeMillis();
		String rowKey = uid + "_" + ts;

		// 生成Put对象
		Put put = new Put(Bytes.toBytes(rowKey));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("content"), Bytes.toBytes(content));

		// 向内容表添加数据
		conTable.put(put);

		// 从关系表中获取用户的fans uid，并封装为Put对象列表
		Get get = new Get(Bytes.toBytes(uid));
		get.addFamily(Bytes.toBytes("fans"));
		Result result = relaTable.get(get);
		Cell[] cells = result.rawCells();
		if (cells.length <= 0) {
			return;
		}

		// 更新fans收件箱表
		List<Put> puts = new ArrayList<>();
		for (Cell cell : cells) {
			byte[] cloneQualifier = CellUtil.cloneQualifier(cell);
			Put inboxPut = new Put(cloneQualifier);
			inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uid"), ts, Bytes.toBytes(rowKey));
			puts.add(inboxPut);
		}
		inboxTable.put(puts);

		// 关闭资源
		inboxTable.close();
		relaTable.close();
		conTable.close();
		connection.close();
	}

	/**
	 * 关注用户 
	 * 1.在用户关系表中 
	 * 	--添加操作人的attends 
	 * 	--添加被关注人的fans 
	 * 2.在收件箱表中
	 * 	--在内容表中获取被关注者的3条数据（rowKey） 
	 * 	--在收件箱表中添加操作人新关注用户的数据
	 * @param uid
	 * @param uids
	 * @throws IOException 
	 */
	public static void addAttend(String uid, String... uids) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);

		// 获取操作的表对象
		Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
		Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
		Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
		
		// 创建操作者的Put对象
		Put relaPut = new Put(Bytes.toBytes(uid));
		List<Put> puts = new ArrayList<>();
		for (String s : uids) {
			relaPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(s), Bytes.toBytes(s));
			// 创建fans的Put对象
			Put fansPut = new Put(Bytes.toBytes(s));
			fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
			puts.add(fansPut);
		}
		puts.add(relaPut);
		relaTable.put(puts);
		
		Put inboxPut = new Put(Bytes.toBytes(uid));
		// 获取内容表中被关注者的rowKey
		for (String s : uids) {
			Scan scan = new Scan(Bytes.toBytes(s), Bytes.toBytes(s + "|"));
			ResultScanner results = conTable.getScanner(scan);
			for (Result result : results) {
				String rowKey = Bytes.toString(result.getRow());
				String[] split = rowKey.split("_");
				byte[] row = result.getRow();
				inboxPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes(s), Long.parseLong(split[1]), row);
			}
		}
		inboxTable.put(inboxPut);
		
		// 关闭资源
		inboxTable.close();
		relaTable.close();
		conTable.close();
		connection.close();
	}

	/**
	 * 取关用户
	 * 1.用户关系表
	 * 	--删除操作者关注列族的待取关用户
	 * 	--删除待取关用户fans列族的操作者
	 * 2.收件箱表
	 * 	--删除操作者的待取关用户的信息
	 * @throws IOException 
	 */
	public static void delAttend(String uid,String ... uids) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);

		// 获取操作的表对象
		Table relaTable = connection.getTable(TableName.valueOf(Constant.RELATIONS));
		Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
		
		// 创建操作者的删除对象
		Delete relaDel = new Delete(Bytes.toBytes(uid));
		List<Delete> deletes = new ArrayList<>();
		for (String s : uids) {
			// 创建被取关者删除对象
			Delete fansDel = new Delete(Bytes.toBytes(s));
			fansDel.addColumns(Bytes.toBytes("fans"), Bytes.toBytes(uid));
			deletes.add(fansDel);
			relaDel.addColumns(Bytes.toBytes("attends"), Bytes.toBytes(s));
		}
		deletes.add(relaDel);
		relaTable.delete(deletes);
		
		// 删除收件箱表相关内容
		Delete inboxDel = new Delete(Bytes.toBytes(uid));
		for (String s : uids) {
			inboxDel.addColumns(Bytes.toBytes("info"), Bytes.toBytes(s));
		}
		inboxTable.delete(inboxDel);
		
		// 关闭资源
		inboxTable.close();
		relaTable.close();
		connection.close();
	}
	

	// 获取微博内容（初始化页面）
	public static void getInit(String uid) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);

		// 获取操作的表对象
		Table inboxTable = connection.getTable(TableName.valueOf(Constant.INBOX));
		Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
		
		// 获取收件箱表数据
		Get get = new Get(Bytes.toBytes(uid));
		get.setMaxVersions();
		Result result = inboxTable.get(get);
		ArrayList<Get> gets = new ArrayList<>();
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			Get conGet = new Get(CellUtil.cloneValue(cell));
			gets.add(conGet);
		}
		
		// 根据收件箱获取值去往内容表获取微博内容
		Result[] results = conTable.get(gets);
		for (Result result2 : results) {
			Cell[] cells2 = result2.rawCells();
			//遍历并打印
			for (Cell cell : cells2) {
				System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
						+ ",Content:" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		
		// 关闭资源
		conTable.close();
		inboxTable.close();
		connection.close();
	}

	// 获取微博内容（查看某个人所有微博内容）
	public static void getData(String uid) throws IOException {
		// 创建连接
		Connection connection = ConnectionFactory.createConnection(configuration);

		// 获取操作的表对象
		Table conTable = connection.getTable(TableName.valueOf(Constant.CONTENT));
		
		// 扫描
		Scan scan = new Scan();
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(uid + "_"));
		scan.setFilter(rowFilter);
		ResultScanner results = conTable.getScanner(scan);
		
		// 遍历打印
		for (Result result : results) {
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
						+ ",Content:" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		
		// 关闭资源
		conTable.close();
		connection.close();
	}

}
