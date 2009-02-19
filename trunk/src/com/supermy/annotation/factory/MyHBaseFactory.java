package com.supermy.annotation.factory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.HColumnDescriptor.CompressionType;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Many2One;
import com.supermy.annotation.One2Many;
import com.supermy.annotation.Table;
import com.supermy.annotation.Transient;
import com.supermy.annotation.test.TableUtil;
import com.supermy.utils.MyHbaseUtil;

/**
 * @author my<br>
 *         注解实现hbase数据库的创建<br>
 */
public class MyHBaseFactory {
	private static final Log log = LogFactory.getLog(MyHBaseFactory.class);

	private HBaseAdmin admin = MyHbaseUtil.getAdmin();
	private HBaseConfiguration hbc = MyHbaseUtil.getConfig();

	public MyHBaseFactory() {
		super();
	}

	public void shutdown() {
		try {
			admin.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 生成hbase数据结构文件
	 * 
	 * @param type
	 * @param htd
	 * @return
	 */
	public HTableDescriptor annotation2hbase(Class<?> type) {
		// 类处理
		// 表的描述
		HTableDescriptor htd = null;
		if (type.isAnnotationPresent(Table.class)) {
			Table t = type.getAnnotation(Table.class);
			htd = new HTableDescriptor(TableUtil.getTable(type));
			htd.setInMemory(t.inMemory());
			htd.setReadOnly(t.readOnly());
		} else {
			return null;
		}

		List<Field> list = new ArrayList<Field>();
		list = walk4field(type, list);
		// 字段的处理
		for (Field field : list) {
			log.debug("field:name======" + field.getName());
			HColumnDescriptor hcd = null;

			// ID暂不处理
			if (field.isAnnotationPresent(ID.class)) {
				ID id = field.getAnnotation(ID.class);
				log.debug(id);

				// ID字段不进行其他的处理
				continue;
			}

			if (field.isAnnotationPresent(Transient.class)) {
				continue;
			}

			if (field.isAnnotationPresent(Column.class)) {
				String fieldname = field.getName();
				String name = fieldname.contains(":") ? fieldname : fieldname
						+ ":";

				Column column = field.getAnnotation(Column.class);

				if (htd.getFamily(name.getBytes()) != null) {
					log.error(name + " repeat !");
					throw new RuntimeException(name + " repeat !");
				}
				hcd = new HColumnDescriptor(name);
				hcd.setBloomfilter(column.bloomfilter());
				hcd.setCompressionType(column.compressionType());
				hcd.setBlockCacheEnabled(column.blockCacheEnabled());
				hcd.setMaxVersions(column.maxVersions());
				if (!htd.isInMemory()) {
					hcd.setInMemory(column.inMemory());
				}
				// continue;
			}

			// 没有注释的使用默认Column的值
			if (!(field.isAnnotationPresent(ID.class)
					|| field.isAnnotationPresent(Transient.class)
					|| field.isAnnotationPresent(Column.class)
					|| field.isAnnotationPresent(One2Many.class) || field
					.isAnnotationPresent(Many2One.class))) {

				String fieldname = field.getName();
				if (!(field.getType().equals(Log.class) || fieldname
						.equalsIgnoreCase("serialVersionUID"))) {
					String name = fieldname.contains(":") ? fieldname
							: fieldname + ":";

					// Column column = field.getAnnotation(Column.class);

					if (htd.getFamily(name.getBytes()) != null) {
						log.error(name + " repeat !");
						throw new RuntimeException(name + " repeat !");
					}
					hcd = new HColumnDescriptor(name);
					// hcd.setBloomfilter(true);
					hcd.setCompressionType(CompressionType.NONE);
					// hcd.setBlockCacheEnabled(column.blockCacheEnabled());
					hcd.setMaxVersions(2);
					// if (!htd.isInMemory()) {
					// hcd.setInMemory(column.inMemory());
					// }
					// continue;
				}

			}

			// else
			// continue;// 字段必选

			if (field.isAnnotationPresent(One2Many.class)) {
				One2Many o2m = field.getAnnotation(One2Many.class);

				log.debug(o2m);
				// continue;
			}
			if (field.isAnnotationPresent(Many2One.class)) {
				Many2One m2o = field.getAnnotation(Many2One.class);
				log.debug(m2o);
				// continue;
			}
			if (hcd != null) {
				htd.addFamily(hcd);
			}
		}
		return htd;
	}

	/**
	 * 
	 * 递归读取所有字段（含父类，可以重复）
	 * 
	 * @param class1
	 * @param list
	 * @return
	 */
	private List<Field> walk4field(Class<?> class1, List<Field> list) {
		Field[] fields = class1.getDeclaredFields();
		for (Field field : fields) {
			list.add(field);
		}
		if (class1.getSuperclass() != null) {
			list = walk4field(class1.getSuperclass(), list);
		}
		return list;
	}

	/**
	 * @param class1
	 * @return
	 */
	private Map<String, Field> getfields(Class<?> class1) {
		Map<String, Field> result = new HashMap<String, Field>();
		List<Field> list = new ArrayList<Field>();
		list = walk4field(class1, list);
		for (Field field : list) {
			Column annotation = field.getAnnotation(Column.class);
			result.put(field.getName(), field);
		}
		return result;
	}

	/**
	 * 获取某个包下的class
	 * 
	 * @param pckgname
	 * @param clazz
	 * @return
	 */
	private Set<Class<?>> getClasses(String pckgname) {
		Set<Class<?>> res = new HashSet<Class<?>>();
		// String pckgname = "test.package.test";
		// pckgname = packageName;
		String name = new String(pckgname);
		if (!name.startsWith("/")) {
			name = "/" + name;
		}
		name = name.replace('.', '/');

		URL url = this.getClass().getResource(name);
		if (url == null) {
			return res;
		}
		File directory = new File(url.getFile());

		if (directory.exists()) {

			String[] files = directory.list();
			for (int i = 0; i < files.length; i++) {

				if (files[i].endsWith(".class")) {

					String classname = files[i].substring(0,
							files[i].length() - 6);
					try {
						String clsName = pckgname + "." + classname;
						log.debug("clsName=" + clsName);
						Class<?> forName = Class.forName(clsName);
						res.add(forName);
					} catch (ClassNotFoundException cnfex) {
						log.error("getClasses(String)" + cnfex, cnfex);
					}
				}
			}
		}

		return res;
	}

	/**
	 * 某个jar文件中某个包的class
	 * 
	 * @param jarFileName
	 * @param packageName
	 * @return
	 */
	private Set<Class<?>> getClasses4Jar(String jarFileName, String packageName) {
		Set<Class<?>> s = new HashSet<Class<?>>();
		try {
			JarFile jarFile = new JarFile(jarFileName);
			Enumeration<JarEntry> enum1 = jarFile.entries();

			for (; enum1.hasMoreElements();) {
				ZipEntry entry = (ZipEntry) enum1.nextElement();
				String entryName = entry.getName().replace('/', '.');
				if (isPackage(entryName, packageName)) {
					// System.out.println(entryName);
					s.add(Class.forName(entryName));
				}
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return s;
	}

	private boolean isPackage(String entryName, String packageName) {
		return entryName.matches("^" + packageName + "\\.[^.]+\\.class");
	}

	public void createMyHBae(String packageName) {
		Set<Class<?>> classes2 = getClasses(packageName);
		try {
			for (Class<?> class1 : classes2) {
				HTableDescriptor line = annotation2hbase(class1);
				if (line != null) {
					autoObj2HBase(line);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public void autoObj2HBase(HTableDescriptor lineTableDesc)
			throws MasterNotRunningException, IOException {
		byte[] tableName = lineTableDesc.getName();
		if (!admin.tableExists(tableName)) {
			log.debug("create ...");
			admin.createTableAsync(lineTableDesc);
		}
		// else {
		log.debug("update ..." + new String(tableName));
		if (!admin.isTableEnabled(tableName)) {
			admin.enableTable(tableName);
		}

		// HTable htable = getTable(tableName);
		HTable htable = MyHbaseUtil.getTable(new String(tableName));
		Map<String, HColumnDescriptor> f1db = new HashMap<String, HColumnDescriptor>();
		for (HColumnDescriptor columnDescriptor : htable.getTableDescriptor()
				.getFamilies()) {
			f1db.put(columnDescriptor.getNameAsString(), columnDescriptor);
		}
		Map<String, HColumnDescriptor> f4db = new HashMap<String, HColumnDescriptor>();
		f4db.putAll(f1db);

		Map<String, HColumnDescriptor> f2obj = new HashMap<String, HColumnDescriptor>();
		for (HColumnDescriptor columnDescriptor : lineTableDesc.getFamilies()) {
			f2obj.put(columnDescriptor.getNameAsString(), columnDescriptor);
		}
		Map<String, HColumnDescriptor> f3obj = new HashMap<String, HColumnDescriptor>();
		f3obj.putAll(f2obj);

		if (admin.isTableEnabled(tableName)) {
			admin.disableTable(tableName);
		}

		removeColumn(tableName, f4db, f3obj);
		addColumn(tableName, f2obj, f1db);
		updateColumn(tableName, htable.getTableDescriptor(), f3obj, f1db);

		if (!admin.isTableEnabled(tableName)) {
			admin.enableTable(tableName);
		}

		// }
	}

	/**
	 * MyHbaseUtil.getTable replace this method
	 * 
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	@Deprecated
	public HTable getTable(byte[] tableName) throws IOException {
		HTable htable = new HTable(hbc, tableName);
		return htable;
	}

	/**
	 * MyHbaseUtil.getTable replace this method
	 * 
	 * @param tableName
	 * @return
	 * @throws IOException
	 */
	@Deprecated
	public HTable getTable(String tableName) throws IOException {
		return getTable(tableName.getBytes());
	}

	private void updateColumn(byte[] tableName, HTableDescriptor dbTableDesc,
			Map<String, HColumnDescriptor> obj,
			Map<String, HColumnDescriptor> db) throws IOException {
		// 依存在，变更的列
		obj.keySet().retainAll(db.keySet());
		for (Entry<String, HColumnDescriptor> entry : obj.entrySet()) {
			HColumnDescriptor dbfamily = dbTableDesc.getFamily(entry.getValue()
					.getName());
			if (!entry.getValue().equals(dbfamily)) {
				log.debug("update column:" + entry.getKey());
				admin.modifyColumn(tableName, entry.getValue().getName(), entry
						.getValue());
			}
		}
	}

	private void addColumn(byte[] tableName,
			Map<String, HColumnDescriptor> obj,
			Map<String, HColumnDescriptor> db) throws IOException {
		// 增加的列
		obj.keySet().removeAll(db.keySet());
		for (Entry<String, HColumnDescriptor> entry : obj.entrySet()) {
			admin.addColumn(tableName, entry.getValue());
		}
	}

	private void removeColumn(byte[] tableName,
			Map<String, HColumnDescriptor> db,
			Map<String, HColumnDescriptor> obj) throws IOException {
		// 删除的列
		db.keySet().removeAll(obj.keySet());
		for (Entry<String, HColumnDescriptor> entry : db.entrySet()) {
			admin.deleteColumn(tableName,
					(entry.getValue().getNameAsString() + ":").getBytes());// FIXME
		}
	}

}
