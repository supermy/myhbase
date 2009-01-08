/**
 * 
 */
package com.supermy.domain.test;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.supermy.domain.User;

/**
 * @author my
 * 
 */
public class ActionTest {
	private final static Log log = LogFactory.getLog(ActionTest.class);

	@Test
	public void crud() throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		log.debug("start create hbase record... ...");
		// create
		User newu = new User("tiger");
		newu.setId("xyz");
		newu.setAge("43");
		newu.setContact("email:;qq:;msn:;");
		newu.setSex("男");
		newu.saveOrUpdate();
		// get
		User uu = new User(null);
		uu.setId("xyz");
		uu.get();
		Assert.assertEquals(uu.getName(), "tiger");
		log.debug(uu);
		// update
		uu.setName("pig");
		uu.saveOrUpdate();
		uu.get();
		Assert.assertEquals(uu.getName(), "pig");
		log.debug(uu);
		// delete
		uu.delete();
		User u3 = new User(null);
		u3.setId("xyz");
		Object object = u3.get();
		log.debug(u3);
		Assert.assertNull(object);
	}

	@Test
	public void PropertyUtils() throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		User u = new User("tiger");
		u.setId("xyz");
		Map<String, String> describe = PropertyUtils.describe(u);
		log.debug(describe);
		Assert.assertEquals(describe.get("id"), "xyz");
	}

	@Test
	public void BeanUtils() throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		User u = new User("tiger");
		u.setId("xyz");
		Map<String, String> describe = BeanUtils.describe(u);
		log.debug(describe);
		Assert.assertEquals(describe.get("id"), "xyz");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
	}

}
