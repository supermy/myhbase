package com.supermy.domain;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.RowResult;

import com.supermy.annotation.test.TableUtil;
import com.supermy.utils.ConvertBean;
import com.supermy.utils.MyHbaseUtil;

/**
 * @author my
 * 
 *         单个对象的生命周期
 */
public  class Action extends Base {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final Log log = LogFactory.getLog(Action.class);

	@Deprecated
	public static class CurrentClassGetter extends SecurityManager {
		public String getClassName() {
			log.debug(getClassContext()[1].getName());
			return getClassContext()[1].getName();
		}
	}
	
	@Deprecated
	public static String  getClassName1() {
		StackTraceElement[] stackTraceElement = new Throwable().getStackTrace();
		String className = stackTraceElement[stackTraceElement.length - 1]
				.getClassName();
		log.debug(className);
		return className;

	}	
	
	@Deprecated
	public static String getClassName() {
		//String className = new CurrentClassGetter().getClassName();
//		String className = test();
		String className = null;
		try {
			throw new Exception();
		} catch (Exception e) {
			StackTraceElement[] element = e.getStackTrace();
			className = element[0].getClassName();
		}
		log.debug(className);
		return className;
	}

	@Deprecated
	public static String getTableName() {

		Class<?> clazz = getThisClass();
		String tablename = clazz.getName().replace(".", "_");
		// Table t = clazz.getAnnotation(Table.class);
		// return t.name();
		return tablename;
	}

	@Deprecated
	public static Class<? extends Action> getThisClass() {
		Class<? extends Action> clazz;
		try {
			clazz = (Class<? extends Action>) Class.forName(getClassName());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		log.debug(clazz);
		return clazz;
	}

	public void commit(BatchUpdate line) {
		String tablename =TableUtil.getTable(getClass());
		//getClass().getName().replace(".", "_");
		HTable htable = MyHbaseUtil.getTable(tablename);
		try {
			htable.commit(line);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public static void commit(List<BatchUpdate> lines) {
		HTable htable = MyHbaseUtil.getTable(getTableName());
		try {
			htable.commit(lines);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 保存
	 */
	public void saveOrUpdate() {
		ConvertBean cb = new ConvertBean();
		BatchUpdate line = cb.object2hbaswrow(this);
		commit(line);
	}

	public static void saveOrUpdate(List<Action> list) {
		ConvertBean cb = new ConvertBean();
		List<BatchUpdate> results = cb.objects2hbaswrows(list);
		commit(results);
	}

	public void delete() {
		try {
			Class<? extends Action> clazz = getClass();
			String tablename =TableUtil.getTable(clazz);
			HTable htable = MyHbaseUtil.getTable(tablename);
			String idValue = getId();
			if (StringUtils.isEmpty(idValue)) {
				throw new RuntimeException("id don't is null!");
			}
			htable.deleteAll(idValue);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	public static void delete(List<Action> list) {//FIXME
		try {

			Class<?> clazz = getThisClass();
			String tablename = TableUtil.getTable(clazz);

			HTable htable = MyHbaseUtil.getTable(tablename);

			for (Action action : list) {
				String idValue = action.getId();
				if (StringUtils.isEmpty(idValue)) {
					throw new RuntimeException("id don't is null!");
				}
				htable.deleteAll(idValue);
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	public Action get() {
		log.debug("get start ... ...Class: " + getClass() + " id:" + getId());
		ConvertBean cb = new ConvertBean();
		Action hbaserow2object = cb.hbaserow2object(this);
		return hbaserow2object;
	}

	public static void get(List<RowResult> list) {
		ConvertBean cb = new ConvertBean();
		cb.hbaserows2objects(list, getThisClass());
	}

}
