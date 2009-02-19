package com.supermy.manager.test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.inject.Guice;
import com.supermy.domain.Action;
import com.supermy.domain.User;
import com.supermy.manager.UserManager;
import com.supermy.utils.MD5;
import com.supermy.utils.MyHBaseException;

/**
 * @author my
 * 
 */
public class MyHBaseTemplateTest {
	private static final Log log = LogFactory.getLog(MyHBaseTemplateTest.class);

	UserManager template = new UserManager();

	// User newu;

	@Test
	public void userRegisterLogin() throws MyHBaseException {
		User newuser = new User();
		newuser.setEmail("springclick@gmail.com");
		newuser.setPassword(MD5.getMD5("111111".getBytes()));
		newuser.setId(MD5.getMD5("springclick@gmail.com".getBytes()));

		Map<String, Object> contact = new HashMap<String, Object>();
		contact.put("contact", "中关村");
		contact.put("name", "tiger");
		contact.put("qq", "123456");
		contact.put("msn", "msn@msn.com");
		contact.put("test1", 12);
		contact.put("test2", Long.parseLong("123456"));
		Date value = new Date();
		contact.put("test3", value);

		newuser.setContact(contact);

		newuser.delete();
		template.register(newuser);
		Assert.assertTrue(template.login(newuser));
	}

	
	@Test
	public void changePwd() throws MyHBaseException {
		User newuser = new User("springclick@gmail.com");
		String oldpassword = MD5.getMD5("111111".getBytes());
		String newpassword = MD5.getMD5("123456".getBytes());

		String id = MD5.getMD5("springclick@gmail.com".getBytes());
		newuser.setId(id);
		newuser.setPassword(oldpassword);
		// 恢复原有口令，便于测试
		newuser.saveOrUpdate();

		template.changePwd(newuser, newpassword);

		User u = (User) newuser.get();
		Assert.assertEquals(u.getPassword(), newuser.getPassword());
	}

	@Test
	public void changeProfile() throws MyHBaseException {
		User newuser = new User("springclick@gmail.com");
		newuser.setAge(99);
		newuser.setName("yy");
		newuser.saveOrUpdate();
		User u = (User) newuser.get();
		Assert.assertEquals(u.getAge(), newuser.getAge());
		Assert.assertEquals(u.getName(), newuser.getName());
	}

	@Test
	public void findUserByName() throws MyHBaseException {
		log.debug("find user by name ... ...");
		User newuser = new User("springclick@gmail.com");
		newuser.setAge(99);
		newuser.setName("tiger");
		newuser.saveOrUpdate();

		String name = "tiger";
		User u = template.findUserByName(name);

		Assert.assertEquals(u.getAge(), newuser.getAge());
		Assert.assertEquals(u.getName(), newuser.getName());

	}

	@Test
	public void TypeNull() throws MyHBaseException {
		log.debug("find user by name ... ...");
		User u = (User) null;
		String s = (String) null;
		// List<Action> list=new ArrayList<Action>();
		// u=(User)list.get(0);
		Map<String, Action> m = new HashMap<String, Action>();
		u = (User) m.get("");
		Assert.assertTrue(true);
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
		// template = new MyHBaseTemplate();
		//
		// newu = new User("tiger");
		// //newu.setId("xyz");
		// newu.setAge(33);
		// Map<String, Object> contact = new HashMap<String, Object>();
		// contact.put("contact", "中关村");
		// contact.put("email", "yy@yy.com");
		// contact.put("qq", "123456");
		// contact.put("msn", "msn@msn.com");
		// contact.put("test1", 12);
		// contact.put("test2", Long.parseLong("123456"));
		// Date value = new Date();
		// contact.put("test3", value);
		//
		// newu.setContact(contact);
		// newu.setSex("男");
		// newu.saveOrUpdate();

	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		// newu.delete();
	}

}
