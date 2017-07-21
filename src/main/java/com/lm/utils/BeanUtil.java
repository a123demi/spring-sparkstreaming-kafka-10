package com.lm.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * 描述：实体工具类 author qiaobin 2016/10/11 16:25.
 */
public class BeanUtil {

	/**
	 * obj转map
	 * @param obj
	 * @return
	 * @throws Exception
	 */
	public static Map<String, Object> objectToMap(Object obj) throws Exception {
		if (obj == null) {
			return null;
		}
		Map<String, Object> map = new HashMap<String, Object>();
		Field[] declaredFields = obj.getClass().getDeclaredFields();
		Field[] superDeclaredFields = obj.getClass().getSuperclass().getDeclaredFields();
		for (Field field : declaredFields) {
			field.setAccessible(true);
			map.put(field.getName(), field.get(obj));
		}
		for (Field field : superDeclaredFields) {
			field.setAccessible(true);
			map.put(field.getName(), field.get(obj));
		}
		return map;
	}

	/**
	 * map转object
	 * @param map
	 * @param beanClass
	 * @return
	 * @throws Exception
	 */
	public static Object mapToObject(Map<String, Object> map, Class<?> beanClass) throws Exception {
		if (map == null)
			return null;
		Object obj = beanClass.newInstance();
		Field[] fields = obj.getClass().getDeclaredFields();
		for (Field field : fields) {
			int mod = field.getModifiers();
			if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
				continue;
			}
			field.setAccessible(true);
			field.set(obj, map.get(field.getName()));
		}
		Field[] superFields = obj.getClass().getSuperclass().getDeclaredFields();
		for (Field field : superFields) {
			int mod = field.getModifiers();
			if (Modifier.isStatic(mod) || Modifier.isFinal(mod)) {
				continue;
			}
			field.setAccessible(true);
			field.set(obj, map.get(field.getName()));
		}
		return obj;
	}
}
