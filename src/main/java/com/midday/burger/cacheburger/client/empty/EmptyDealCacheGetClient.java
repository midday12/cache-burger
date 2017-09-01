package com.midday.burger.cacheburger.client.empty;

import com.midday.burger.cacheburger.client.BurgerCacheGetClientImpl;
import com.midday.burger.cacheburger.model.BurgerCacheDefinition;
import com.midday.burger.util.EmptyUtil;

import java.util.Collection;
import java.util.Map;

/**
 * Created by midday on 2017-01-11.
 */
public class EmptyDealCacheGetClient extends BurgerCacheGetClientImpl {
	public EmptyDealCacheGetClient() {
	}

	public boolean connect(BurgerCacheDefinition cacheDefinition) {
		return true;
	}

	public void close() {
	}

	public Map<String, Object> getReal(String key) {
		return null;
	}

	protected Map<String, Map<String, Object>> getReal(Collection<String> keys) {
		return EmptyUtil.emptyMap();
	}

	@SuppressWarnings("unchecked")
	protected void putReal(String key, Map<String, Object> value) {
	}

	@SuppressWarnings("unchecked")
	protected void putReal(Map<String, Map<String, Object>> keyValue) {
	}

	protected boolean removeReal(String key) {
		return true;
	}
	protected void removeReal(Collection<String> keys) {
	}
}
