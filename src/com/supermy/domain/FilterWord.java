package com.supermy.domain;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.supermy.annotation.Column;
import com.supermy.annotation.Table;

@Table(name = "filterword")
public class FilterWord {
	private static final Log log = LogFactory.getLog(FilterWord.class);

	@Column(name = "keyword", inMemory = true)
	private String keyword;

	/**
	 * @return the keyword
	 */
	public String getKeyword() {
		return keyword;
	}

	/**
	 * @param keyword
	 *            the keyword to set
	 */
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public static boolean filterWords(String content, String... filterWords) {
		// 敏感词库 可编辑
		String[] forbiddenArray = null;
		if (filterWords.length <= 0) {
			forbiddenArray = new String[] { "色情", "他妈的", "王八蛋", "黄色", "操", "办证" };

		} else {
			forbiddenArray = filterWords;
		}

		String sp = "";
		for (String line : forbiddenArray) {
			sp = sp + "" + line + "|";
		}
		// sp = new String(StringUtils.left(sp, sp.length() - 1));
		sp = new String(sp.substring(0, sp.length() - 1));
		Pattern p = Pattern.compile(sp, Pattern.CASE_INSENSITIVE);
		Matcher matcher = p.matcher(content);
		if (matcher.find()) {
			log.debug(content);
			log.debug("pattern:" + sp);
			log.debug(matcher.group());
			return true;
		} else
			return false;
	}

}
