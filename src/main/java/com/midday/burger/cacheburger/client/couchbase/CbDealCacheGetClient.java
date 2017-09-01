package com.midday.burger.cacheburger.client.couchbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.midday.burger.cacheburger.client.BurgerCacheGetClientImpl;
import com.midday.burger.cacheburger.model.BurgerCacheDefinition;
import com.midday.burger.couchbaseburger.BurgerCbDatabaseClient;
import com.midday.burger.couchbaseburger.BurgerCbDocument;
import com.midday.burger.util.EmptyUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * Created by midday on 2017-01-11.
 */
@Slf4j
public class CbDealCacheGetClient extends BurgerCacheGetClientImpl {
	protected BurgerCbDatabaseClient cbCacheBucket;

	public CbDealCacheGetClient(BurgerCacheDefinition cacheDefinition) {
		connect(cacheDefinition);
	}

	public CbDealCacheGetClient(BurgerCacheDefinition cacheDefinition, int remoteCacheAsyncTime, ExecutorService esAsyncCacheExecutor, BurgerCacheGetClientImpl localCacheClientToSync) {
		connect(cacheDefinition);

		this.remoteCacheAsyncTime = remoteCacheAsyncTime;
		this.esAsyncCacheExecutor = esAsyncCacheExecutor;
		this.localCacheClientToSync = localCacheClientToSync;
	}

	public boolean connect(BurgerCacheDefinition cacheDefinition) {
		try {
			this.cbCacheBucket = new BurgerCbDatabaseClient(cacheDefinition.getCouchbaseNode(), cacheDefinition.getCouchbaseBucket(), cacheDefinition.getCouchbasePassword());
			this.cacheDefinition = cacheDefinition;

			return true;
		} catch(Exception e) {
			log.error(e.toString());
		}

		return false;
	}

	public void close() {
		// Couchbase는 Connection close를 별도로 하지 않아도 된다.
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Object> getReal(String key) {
		try {
			return cbCacheBucket.exist(key)
					.filter(ret -> ret)
					.flatMap(ret -> {
						try {
							return cbCacheBucket.get(key, Map.class);
						} catch(Exception e) {
							log.error("CbDealCacheClient.getReal : " + e.toString());
						}
						return rx.Observable.empty();
					})
					.map(dataMap -> (Map<String, Object>)dataMap)
					.toBlocking().singleOrDefault(null);
		} catch(Exception e) {
			log.error("CbDealCacheClient.getReal : " + e.toString());
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	protected Map<String, Map<String, Object>> getReal(Collection<String> keys) {
		try {
			Map<String, BurgerCbDocument> cbDocMap = (Map<String, BurgerCbDocument>)cbCacheBucket.gets(new ArrayList(keys)).toBlocking().single();

			return rx.Observable.from(cbDocMap.entrySet())
					.toMap(entry -> entry.getKey(), entry -> entry.getValue().getPropertyMap())
					.toBlocking().single();
		} catch(Exception e) {
			log.error("CbDealCacheClient.get : " + e.toString());
		}

		return EmptyUtil.emptyMap();
	}

	protected void putReal(String key, Map<String, Object> value) {
		if(cbCacheBucket == null) return;

		// Couchbase 특성상, TTL이 30일 이상이면 UNIX TIME으로 지정되어야 함
		int cacheTime = cacheDefinition.getRemoteCacheTime();
		if(cacheTime > (30 * 24 * 60 * 60)) {
			cacheTime += (int)(System.currentTimeMillis() / 1000L);
		}

		try {
			cbCacheBucket.put(key, value, Map.class, cacheTime).toBlocking().single();
		} catch (Exception e) {
			log.error("CbDealCacheClient.put : " + e.toString());
		}
	}

	protected void putReal(Map<String, Map<String, Object>> keyValue) {
		if(cbCacheBucket == null) return;

		// Couchbase 특성상, TTL이 30일 이상이면 UNIX TIME으로 지정되어야 함
		int cacheTime = cacheDefinition.getRemoteCacheTime();
		if(cacheTime > (30 * 24 * 60 * 60)) {
			cacheTime += (int)(System.currentTimeMillis() / 1000L);
		}

		List<BurgerCbDocument> cbDocList = keyValue.entrySet().stream()
				.map(entry -> {
					try {
						return new BurgerCbDocument(entry.getKey(), new ObjectMapper().writeValueAsString(entry.getValue()));
					} catch (Exception e) {
						log.error("CbDealCacheClient.put : " + e.toString());
						return null;
					}
				})
				.filter(cbDoc -> cbDoc != null)
				.collect(Collectors.toList());

		try {
			cbCacheBucket.puts(cacheTime, cbDocList).toBlocking().single();
		} catch (Exception e) {
			System.err.println(e.toString());
		}
	}

	protected boolean removeReal(String key) {
		if(cbCacheBucket == null) return false;

		try {
			return cbCacheBucket.remove(key).toBlocking().single();
		} catch (Exception e) {
			log.error("CbDealCacheClient.remove : " + e.toString());
		}

		return false;
	}

	protected void removeReal(Collection<String> keys) {
		if(cbCacheBucket == null) return;

		keys.forEach(this::removeReal);
	}
}
