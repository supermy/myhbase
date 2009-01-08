/**
 * 
 */
package com.supermy.annotation.test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.supermy.annotation.factory.MyHBaseFactory;
import com.supermy.domain.User;
import com.supermy.utils.MyHbaseUtil;

/**
 * @author my
 * 
 */
public class MyHBaseFactoryTest {
	private static final Log log = LogFactory.getLog(MyHBaseFactoryTest.class);

	MyHBaseFactory fac = new MyHBaseFactory();

	@Test
	public void inheritedInstanceField() {
		User u = new User("tiger");
		Field[] fields = u.getClass().getDeclaredFields();
		for (Field field : fields) {
			log.debug(field);
		}
	}

	@Test
	public void inheritedAannotation2hbase() {
		HTableDescriptor u = fac.annotation2hbase(User.class);
		log.debug(u);
		Collection<HColumnDescriptor> families = u.getFamilies();
		for (HColumnDescriptor columnDescriptor : families) {
			log.debug(columnDescriptor.getNameAsString());
		}
	}

	@Test
	public void autoMany() throws ClassNotFoundException {
		fac.createMyHBae(User.class.getPackage().getName());
	}

	@Test
	public void autoAddModyfyUpdate() throws ClassNotFoundException,
			MasterNotRunningException, IOException {
		HTableDescriptor u = fac.annotation2hbase(User.class);
		HColumnDescriptor family = new HColumnDescriptor("test1:");
		u.addFamily(family);
		fac.autoObj2HBase(u);

		HTable table = MyHbaseUtil.getTable("user_test");// fac.getTable("user_test");
		Assert.assertEquals(table.getTableDescriptor().getFamily(
				family.getName()), family);

		u.removeFamily(family.getName());
		fac.autoObj2HBase(u);
		Assert.assertNull(table.getTableDescriptor()
				.getFamily(family.getName()));

	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		fac = new MyHBaseFactory();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		// MyHBaseFactory fac = new MyHBaseFactory();
		// fac.shutdown();
	}

}
