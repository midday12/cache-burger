package com.midday.burger.cacheburger.client;

import com.midday.burger.cacheburger.model.BurgerCacheDefinition;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

/**
 * Created by midday on 2017-01-11.
 */
public interface BurgerCacheGetClient {
	boolean connect(BurgerCacheDefinition cacheDefinition);
	void close();

	<T, R> R get(T key, Function<T, R> getFunction);
	<T, R> Map<T, R> gets(Collection<T> keys, Function<Collection<T>, Map<T, R>> getFunction);
	<T, R> void put(T key, R value);
	<T, R> void puts(Map<T, R> keyValue);
	<T> boolean remove(T key);
	<T> void removes(Collection<T> keys);
}
