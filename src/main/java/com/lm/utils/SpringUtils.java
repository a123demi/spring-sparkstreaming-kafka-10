package com.lm.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

final public class SpringUtils {

	private static ApplicationContext ac = null;

	private SpringUtils() {

	}

	static {
		ac = new ClassPathXmlApplicationContext("applicationContext.xml");
	}

	public static ApplicationContext getApplicationContext() {
		// 获得返回的容器对象
		return ac;
	}

}