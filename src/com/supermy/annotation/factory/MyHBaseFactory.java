package com.supermy.annotation.factory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Many2One;
import com.supermy.annotation.One2Many;
import com.supermy.annotation.Table;

/**
 * @author my 注解实现hbase数据库的创建
 */
public class MyHBaseFactory {
	private static final Log log = LogFactory.getLog(MyHBaseFactory.class);

	private HBaseAdmin admin;

	//	
	public MyHBaseFactory() {
		super();
		ResourceBundle bundle = ResourceBundle.getBundle("myhbase");
		String value = bundle.getObject("hbase.master").toString();
		HBaseConfiguration hbc = new HBaseConfiguration();
		hbc.set("hbase.master", value);
		try {
			admin = new HBaseAdmin(hbc);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			log.error(e);
			throw new RuntimeException(e);
		}
	}

	private HTableDescriptor annotation2hbase(Class<?> class1) {
		// 类处理
		// 表的描述
		HTableDescriptor htd = null;
		if (class1.isAnnotationPresent(Table.class)) {
			Table t = class1.getAnnotation(Table.class);
			htd = new HTableDescriptor(t.name());
			htd.setInMemory(t.inMemory());
			htd.setReadOnly(t.readOnly());
		} else {
			return null;
		}
		// 字段的处理
		Field[] fields = class1.getDeclaredFields();
		for (Field field : fields) {
			log.debug("field:name======" + field.getName());
			HColumnDescriptor hcd = null;

			// ID暂不处理
			if (field.isAnnotationPresent(ID.class)) {
				ID id = field.getAnnotation(ID.class);
				log.debug(id);

				// ID字段不进行其他的处理
				continue;
			}

			if (field.isAnnotationPresent(Column.class)) {
				Column column = field.getAnnotation(Column.class);
				String name = column.name().contains(":") ? column.name()
						: column.name() + ":";
				hcd = new HColumnDescriptor(name);
				hcd.setBloomfilter(column.bloomfilter());
				hcd.setCompressionType(column.compressionType());
				hcd.setBlockCacheEnabled(column.blockCacheEnabled());
				hcd.setMaxVersions(column.maxVersions());
				if (!htd.isInMemory()) {
					hcd.setInMemory(column.inMemory());
				}
			} else
				continue;// 字段必选

			if (field.isAnnotationPresent(One2Many.class)) {
				One2Many o2m = field.getAnnotation(One2Many.class);

				log.debug(o2m);
			}
			if (field.isAnnotationPresent(Many2One.class)) {
				Many2One m2o = field.getAnnotation(Many2One.class);
				log.debug(m2o);

			}
			htd.addFamily(hcd);
		}
		return htd;
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
				log.debug(line);
				admin.createTableAsync(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
