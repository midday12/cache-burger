package com.midday.burger.util;

import org.apache.commons.collections4.map.UnmodifiableMap;

import java.util.*;

/**
 * Created by midday on 2017-08-31.
 */
public class EmptyUtil {
	public static <T> boolean collectionEmpty(Collection<T> collection) {
		if(collection == null) return true;

		return collection.isEmpty();
	}

	public static boolean isEmpty(Map m) {
		if(m == null) return true;

		return m.isEmpty();
	}

	public static boolean isEmpty(String s) {
		if(s == null) return true;

		return s.isEmpty();
	}

	public static Map emptyMap() {
		return UnmodifiableMap.unmodifiableMap(new HashMap());
	}

	public static List emptyList() {
		return Collections.EMPTY_LIST;
	}
}
