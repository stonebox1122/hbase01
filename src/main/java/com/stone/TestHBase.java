package com.stone;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
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
import org.apache.hadoop.hbase.util.Bytes;

public class TestHBase {
	
	private static Configuration conf = null;
	private static Connection conn = null;
	private static Admin admin = null;
	
	static {
		//HBaseConfiguration conf = new HBaseConfiguration();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "172.30.60.62");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		//HBaseAdmin admin = new HBaseAdmin(conf);
		//boolean tableExists = admin.tableExists(tableName);
		
		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void close(Connection conn, Admin admin) {
		if (conn != null) {
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (admin != null) {
			try {
				admin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	// 判断表是否存在
	public static boolean tableExists(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		boolean tableExists = admin.tableExists(TableName.valueOf(tableName));
		return tableExists;
	}
	
	//创建表
	public static void createTable(String tableName, String ... cfs) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if (tableExists(tableName)) {
			System.out.println("表已存在！！");
			return;
		}
		//创建表描述器
		HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
		//添加列族
		for (String cf : cfs) {
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
			hTableDescriptor.addFamily(hColumnDescriptor);
		}
		admin.createTable(hTableDescriptor);
		System.out.println("表创建成功！！");
	}
	
	//删除表
	public static void deleteTable(String tableName) throws IOException {
		if (!tableExists(tableName)) {
			return;
		}
		//使表不可用
		admin.disableTable(TableName.valueOf(tableName));
		//执行删除操作
		admin.deleteTable(TableName.valueOf(tableName));
	}
	
	//增&改
	public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
		if (!tableExists(tableName)) {
			return;
		}
		//HTable table = new HTable(conf, TableName.valueOf(tableName));
		//获取表对象
		Table table = conn.getTable(TableName.valueOf(tableName));
		//创建put对象
		Put put = new Put(Bytes.toBytes(rowKey));
		//添加数据
		put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
		//执行添加操作
		table.put(put);
		table.close();
	}
	
	//删除
	public static void deleteData(String tableName, String rowKey, String cf, String cn) throws IOException {
		if (!tableExists(tableName)) {
			return;
		}
		Table table = conn.getTable(TableName.valueOf(tableName));
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
		//delete.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn)); 慎用，只会删除最新版本的数据
		table.delete(delete);
		table.close();
	}
	
	//查询
	public static void scanTable(String tableName) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if (!tableExists(tableName)) {
			return;
		}
		Table table = conn.getTable(TableName.valueOf(tableName));
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		for (Result result : results) {
			Cell[] cells = result.rawCells();
			for (Cell cell : cells) {
				System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
				                 + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
				                 + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
				                 + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
			}
		}
		table.close();
	}
	
	public static void getData(String tableName,String rowKey,String cf,String cn) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		if (!tableExists(tableName)) {
			return;
		}
		Table table = conn.getTable(TableName.valueOf(tableName));
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
//		get.setMaxVersions();
		Result result = table.get(get);
		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			System.out.println("RK:" + Bytes.toString(CellUtil.cloneRow(cell))
			                 + ",CF:" + Bytes.toString(CellUtil.cloneFamily(cell))
			                 + ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell))
			                 + ",VALUE:" + Bytes.toString(CellUtil.cloneValue(cell)));
		}
		table.close();
	}

	public static void main(String[] args) throws Exception {
		
//		System.out.println(tableExists("staff"));
//		createTable("staff", "info");
//		System.out.println(tableExists("staff"));
//		deleteTable("staff");
//		System.out.println(tableExists("staff"));
		
//		putData("student", "1003", "info", "name", "tom");
//		deleteData("student", "1001", "info", "age");
//		scanTable("student");
		getData("student", "1002", "info", "name");
		
		close(conn, admin);
	}

}
