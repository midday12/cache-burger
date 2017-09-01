package com.midday.burger.cacheburger.client.memcached;

import com.midday.burger.cacheburger.client.BurgerCacheGetClientImpl;
import com.midday.burger.cacheburger.model.BurgerCacheDefinition;

import lombok.extern.slf4j.Slf4j;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import org.javatuples.Pair;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by midday on 2017-01-11.
 */
@Slf4j
public class MclCacheGetClient extends BurgerCacheGetClientImpl {
	private MemcachedClient mcClient;

	public MclCacheGetClient(BurgerCacheDefinition cacheDefinition) {
		connect(cacheDefinition);
	}

	public MclCacheGetClient(BurgerCacheDefinition cacheDefinition, int remoteCacheAsyncTime, ExecutorService esAsyncCacheExecutor, BurgerCacheGetClientImpl localCacheClientToSync) {
		connect(cacheDefinition);

		this.remoteCacheAsyncTime = remoteCacheAsyncTime;
		this.esAsyncCacheExecutor = esAsyncCacheExecutor;
		this.localCacheClientToSync = localCacheClientToSync;
	}

	@Override
	public boolean connect(BurgerCacheDefinition cacheDefinition) {
		try {
			mcClient = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(cacheDefinition.getMemcachedServer()));
			if (mcClient != null) {
				this.cacheDefinition = cacheDefinition;
				return true;
			}
		} catch(Exception e) {
			log.error("McCacheClient connection error : " + e.toString());
		}

		return false;
	}

	@Override
	public void close() {
		if(mcClient != null) {
			mcClient.shutdown();
		}
	}

	protected Map<String, Object> getReal(String key) {
		return (Map<String, Object>)mcClient.get(key);
	}

	protected Map<String, Map<String, Object>> getReal(Collection<String> keys) {
		return mcClient.getBulk(keys).entrySet().stream()
				.map(entry -> new Pair<>(entry.getKey(), (Map<String, Object>)entry.getValue()))
				.collect(Collectors.toMap(pair -> pair.getValue0(), pair -> pair.getValue1()));
	}

	protected void putReal(String key, Map<String, Object> value) {
		try {
			mcClient.set(key, cacheDefinition.getRemoteCacheTime() * 1000, value)
					.get(1, TimeUnit.MINUTES);
		} catch(Exception e) {
			log.error("McCacheClient putReal error : " + e.toString());
		}
	}

	protected void putReal(Map<String, Map<String, Object>> keyValue) {
		keyValue.entrySet().forEach(entry -> {
			try {
				mcClient.set(entry.getKey(), cacheDefinition.getRemoteCacheTime() * 1000, entry.getValue())
						.get(1, TimeUnit.MINUTES);
			} catch (InterruptedException | TimeoutException | ExecutionException e) {
				log.error("McCacheClient removeReal delete error : " + e.toString());
			}
		});
	}

	protected boolean removeReal(String key) {
		if(mcClient == null) return false;

		try {
			return mcClient.delete(key).get();
		} catch(InterruptedException | ExecutionException e) {
			log.error("McCacheClient removeReal error : " + e.toString());
		}

		return false;
	}

	protected void removeReal(Collection<String> keys) {
		keys.forEach(key -> {
			try {
				mcClient.delete(key)
						.get(1, TimeUnit.MINUTES);
			} catch (InterruptedException | TimeoutException | ExecutionException e) {
				log.error("McCacheClient removeReal delete error : " + e.toString());
			}
		});
	}
}
