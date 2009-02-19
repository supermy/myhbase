package com.supermy.manager;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.supermy.domain.Action;
import com.supermy.domain.User;
import com.supermy.utils.MD5;
import com.supermy.utils.MyHBaseException;

public class UserManager {
	private static final Log log = LogFactory.getLog(UserManager.class);

	private MyHBaseTemplate<User> userUtil=new MyHBaseTemplate<User>();

	/**
	 * 按用户名称查询用户
	 * 
	 * @param value
	 * @return
	 * @throws MyHBaseException
	 */
	public User findUserByName(String value) throws MyHBaseException {
		String column = "name";
		
		List<Action> find = userUtil.find(User.class, new String[] { column },
				new String[] { value }, "", 1);

		log.debug(find);
		// Assert.assertTrue(find.size()>0);

		if (find.size() <= 0) {
			return null;
		} else {
			return (User) find.get(0);
		}
	}

	public User getUserByEmail(String email) throws MyHBaseException {
		return (User) userUtil.get(User.class, MD5.getMD5(email.getBytes()));
	}

	public void register(User user) throws MyHBaseException {
		// email要唯一
		User userByEmail = getUserByEmail(user.getEmail());
		if (userByEmail != null) {
			throw new MyHBaseException("用户已经存在");
		}
		user.saveOrUpdate();
		log.debug(user.get());
	}

	public boolean login(User newuser) throws MyHBaseException {
		User userByEmail = getUserByEmail(newuser.getEmail());
		if (userByEmail == null) {
			throw new MyHBaseException("用户名不存在！");
		}
		if (!userByEmail.getPassword().equalsIgnoreCase(newuser.getPassword())) {
			throw new MyHBaseException("用户名或者口令不正确！");
		}
		log.debug("成功登录");
		return true;
	}

	public void changePwd(User newuser, String newpassword)
			throws MyHBaseException {
		if (StringUtils.isBlank(newuser.getId())) {
			throw new MyHBaseException("id不能为空......");
		}
		if (StringUtils.isBlank(newpassword)) {
			throw new MyHBaseException("新口令不能为空......");
		}
		if (!login(newuser)) {
			throw new MyHBaseException("非法用户......");
		}
		// 更改口令
		newuser.setPassword(newpassword);
		newuser.saveOrUpdate();
	}

}