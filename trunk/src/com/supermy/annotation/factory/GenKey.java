package com.supermy.annotation.factory;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;

import com.supermy.utils.MyHBaseException;
import com.supermy.utils.MyHbaseUtil;

public class GenKey {
	private HTable primaryKey = MyHbaseUtil.getTable("primaryKeys");

	// public GenKey(HTable primaryKey) {
	// super();
	// this.primaryKey = primaryKey;
	// }

	/**
	 * 构造自动增长主键
	 * 
	 * @param tableName
	 * @return
	 * @throws MyHBaseException
	 */
	public String genAutoIncreasePrimaryKey(String tableName)
			throws MyHBaseException {
		String value = "1";
		try {
			Cell cell = primaryKey.get(tableName, "primarykey:");
			synchronized (value) {
				if (cell != null) {
					value = new String(cell.getValue());
					Integer valueOf = Integer.valueOf(value);
					value += "" + (valueOf + 1);
				}
			}
			// 更新数据库
			BatchUpdate key = new BatchUpdate(tableName);
			key.put("primarykey:", value.getBytes());
			primaryKey.commit(key);
		} catch (IOException e) {
			e.printStackTrace();
			// throw new MyHBaseException(e.getLocalizedMessage());
			throw new RuntimeException(e);
		}
		return value;
	}

	/**
	 * 构造指定key的增长主键（可以作为parent-child的一种使用）
	 * 
	 * @param tableName
	 * @param key
	 * @param x
	 * @return
	 * @throws MyHBaseException
	 */
	public String genAutoIncLoginKey(String tableName, String key, String... x)
			throws MyHBaseException {
		StringBuilder sb = new StringBuilder();
		for (String param : x) {
			sb.append(param);
			sb.append(";");
		}
		sb.append(key);
		sb.append(";");
		sb.append(genAutoIncreasePrimaryKey(tableName));
		return new String(sb);
	}

	public String UUID() throws MyHBaseException {
		return java.util.UUID.randomUUID().toString();
	}

}
