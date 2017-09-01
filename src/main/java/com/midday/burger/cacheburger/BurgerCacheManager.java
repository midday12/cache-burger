package com.midday.burger.cacheburger;

import com.midday.burger.cacheburger.client.BurgerCacheGetClientImpl;
import com.midday.burger.cacheburger.client.couchbase.CbCacheGetClient;
import com.midday.burger.cacheburger.client.ehcache.EhCacheGetClient;
import com.midday.burger.cacheburger.client.empty.EmptyCacheGetClient;
import com.midday.burger.cacheburger.client.memcached.MclCacheGetClient;
import com.midday.burger.cacheburger.model.BurgerCacheDefinition;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Created by midday on 2016-12-28.
 */
public class BurgerCacheManager implements InitializingBean, DisposableBean {
	@Getter @Setter
	protected BurgerCacheDefinition cacheDefinition;

	@Getter @Setter
	protected int remoteCacheAsyncTime = 0;

	@Getter @Setter
	protected int asyncThreadCount = 1;

	protected ExecutorService esAsync = null;

	protected BurgerCacheGetClientImpl lcClient = new EmptyCacheGetClient();
	protected BurgerCacheGetClientImpl rcClient= new EmptyCacheGetClient();

	@Override
	public void afterPropertiesSet() throws Exception {
		if(remoteCacheAsyncTime > 0 && asyncThreadCount > 0) {
			esAsync = Executors.newFixedThreadPool(asyncThreadCount);
		}

		if(!StringUtils.isEmpty(cacheDefinition.getName()) && cacheDefinition.getLocalCacheTime() > 0) {
			lcClient = new EhCacheGetClient(cacheDefinition, remoteCacheAsyncTime, esAsync);
		}

		if(cacheDefinition.getRemoteCacheTime() > 0) {
			if (!StringUtils.isEmpty(cacheDefinition.getMemcachedServer())) {
				rcClient = new MclCacheGetClient(cacheDefinition, remoteCacheAsyncTime, esAsync, lcClient);
			} else if (!StringUtils.isEmpty(cacheDefinition.getCouchbaseNode())) {
				rcClient = new CbCacheGetClient(cacheDefinition, remoteCacheAsyncTime, esAsync, lcClient);
			}
		}
	}

	@Override
	public void destroy() throws Exception {
		if(esAsync != null) {
			esAsync.shutdown();
			esAsync.awaitTermination(1, TimeUnit.MINUTES);

			esAsync = null;
		}

		net.sf.ehcache.CacheManager cacheManager = net.sf.ehcache.CacheManager.getInstance();
		if(cacheManager == null) return;

		cacheManager.removeCache(cacheDefinition.getName());
	}

	/**
	 * 1. 로컬캐시 조회
	 * 2. 로컬캐시에 없고 원격캐시가 설정되어 있을 경우, 원격캐시 조회
	 * 3. 캐시 결과가 없으면, getFunction으로 직접 조회
	 * 4. 조회한 결과를 로컬캐시에 저장 (원격캐시 저장은 원격캐시 조회함수 내에서 처리)

	 * @param key
	 * @param getFunction
	 * @return
	 */
	public <T, R> R get(T key, Function<T, R> getFunction) {
		return lcClient.get(key, (cacheKey) -> {
			return rcClient.get(cacheKey, getFunction);
		});
	}


	/**
	 * 1. 로컬캐시 조회
	 * 2. 로컬캐시에 없고 원격캐시가 설정되어 있을 경우, 원격캐시 조회
	 * 3. 캐시 결과가 없으면, getFunction으로 직접 조회
	 * 4. 조회한 결과를 로컬캐시에 저장 (원격캐시 저장은 원격캐시 조회함수 내에서 처리)
	 *
	 * @param keys
	 * @param getFunction
	 * @return
	 */
	public <T, R> Map<T, R> gets(Collection<T> keys, Function<Collection<T>, Map<T, R>> getFunction) {
		return lcClient.gets(keys, (cacheKeys) -> {
			return rcClient.gets(cacheKeys, getFunction);
		});
	}

	/**
	 * 로컬캐시에 저장된 정보 조회. 없으면 null 리턴
	 *
	 * @param key
	 * @param <T>
	 * @param <R>
	 * @return
	 */
	public <T, R> R getLocalCache(T key) {
		if(lcClient == null) return null;

		return lcClient.get(key, null);
	}

	/**
	 * 원격캐시에 저장된 정보 조회. 없으면 null 리턴
	 *
	 * @param key
	 * @param <T>
	 * @param <R>
	 * @return
	 */
	public <T, R> R getRemoteCache(T key) {
		if(rcClient == null) return null;

		return rcClient.get(key, null);
	}

	/**
	 * 캐시를 즉각 업데이트
	 *
	 * @param key
	 * @param value
	 */
	public <T, R> void updateCache(T key, R value) {
		lcClient.put(key, value);
		rcClient.put(key, value);
	}

	/**
	 * 캐시를 즉각 업데이트
	 *
	 * @param keyValue
	 */
	public <T, R> void updateCache(Map<T, R> keyValue) {
		lcClient.puts(keyValue);
		rcClient.puts(keyValue);
	}

	/**
	 * Cache Hit Count 체크
	 * @return
	 */
	public long[] getCacheHit() {
		long[] count = new long[3];

		count[0] = lcClient.getGetCount();
		count[1] = lcClient.getHitCount();
		count[2] = rcClient.getHitCount();

		return count;
	}
}
