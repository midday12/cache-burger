package com.midday.burger.cacheburger.model;

import lombok.Data;
import lombok.ToString;
import sun.misc.Cache;

/**
 * Created by midday on 2016-12-28.
 */
@Data
@ToString
public class BurgerCacheDefinition<T> {
	private String name;
	protected Class cacheModelClass;

	private int localCacheTime;
	private int remoteCacheTime;

	private int localCacheElementMaxCount;

	private String couchbaseNode;
	private String couchbaseBucket;
	private String couchbasePassword;

	private String memcachedServer;
}
