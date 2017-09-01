package com.midday.burger.cacheburger.client.ehcache;

import com.midday.burger.cacheburger.client.BurgerCacheGetClientImpl;
import com.midday.burger.cacheburger.model.BurgerCacheDefinition;
import com.midday.burger.util.EmptyUtil;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Created by midday on 2017-01-11.
 */
public class EhDealCacheGetClient extends BurgerCacheGetClientImpl {
	protected Cache localCache;

	public EhDealCacheGetClient(BurgerCacheDefinition cacheDefinition) {
		connect(cacheDefinition);
	}

	public EhDealCacheGetClient(BurgerCacheDefinition cacheDefinition, int remoteCacheAsyncTime, ExecutorService esAsyncCacheExecutor) {
		connect(cacheDefinition);

		this.remoteCacheAsyncTime = remoteCacheAsyncTime;
		this.esAsyncCacheExecutor = esAsyncCacheExecutor;
	}

	public boolean connect(BurgerCacheDefinition cacheDefinition) {
		// EhCache 생성
		CacheManager cacheManager = CacheManager.create();
		if(cacheManager == null) return false;

		CacheConfiguration conf = new CacheConfiguration();
		conf.setName(cacheDefinition.getName());
		conf.setMaxEntriesLocalHeap(cacheDefinition.getLocalCacheElementMaxCount());
		conf.setEternal(false);
		conf.setTimeToIdleSeconds(0);
		conf.setTimeToLiveSeconds(cacheDefinition.getLocalCacheTime());
		conf.setMemoryStoreEvictionPolicy("LRU");

		localCache = new Cache(conf);
		cacheManager.addCache(localCache);

		this.cacheDefinition = cacheDefinition;

		return true;
	}

	public void close() {
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getReal(String key) {
		Element elem = localCache.get(key);
		if(elem == null) return null;
		if(elem.getObjectValue() == null) return null;

		return (Map<String, Object>)elem.getObjectValue();
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Map<String, Object>> getReal(Collection<String> keys) {
		Map<String, Map<String, Object>> retMap = new HashMap<>();

		// 먼저 cache에 있는 항목들을 찾는다.
		Map<Object, Element> elems = localCache.getAll(keys);
		if(EmptyUtil.isEmpty(elems) == false) {
			elems.values().forEach(elem -> {
				if(elem != null) {
					if(elem.getObjectValue() != null) {
						retMap.put((String) elem.getObjectKey(), (Map<String, Object>)elem.getObjectValue());
					} else {
						retMap.put((String) elem.getObjectKey(), null);
					}
				}
			});
		}

		return retMap;
	}

	@SuppressWarnings("unchecked")
	protected void putReal(String key, Map<String, Object> value) {
		if(localCache == null) return;

		localCache.put(new Element(key, value));
	}

	@SuppressWarnings("unchecked")
	protected void putReal(Map<String, Map<String, Object>> keyValue) {
		if(localCache == null) return;

		keyValue.forEach((t, r) -> putReal(t, r));
	}

	protected boolean removeReal(String key) {
		if(localCache == null) return false;

		return localCache.remove(key);
	}

	protected void removeReal(Collection<String> keys) {
		if(localCache == null) return;

		localCache.removeAll(keys);
	}
}
