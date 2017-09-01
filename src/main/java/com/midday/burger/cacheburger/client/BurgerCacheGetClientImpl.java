package com.midday.burger.cacheburger.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.midday.burger.cacheburger.model.BurgerCacheDefinition;
import com.midday.burger.util.EmptyUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.javatuples.Pair;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by midday on 2017-01-11.
 */
@Slf4j
public abstract class BurgerCacheGetClientImpl implements BurgerCacheGetClient {
	protected BurgerCacheDefinition cacheDefinition;

	protected ExecutorService esAsyncCacheExecutor = null;
	protected int remoteCacheAsyncTime = 0;
	protected BurgerCacheGetClientImpl localCacheClientToSync = null;     // 비동기 캐시 갱신 시, 동기화해주어야 할 로컬캐시 클라이언트

	protected ObjectMapper mapper = new ObjectMapper();

	@Getter
	protected long getCount = 0;
	@Getter
	protected long hitCount = 0;

	abstract protected Map<String, Object> getReal(String key);
	abstract protected Map<String, Map<String, Object>> getReal(Collection<String> keys);

	abstract protected void putReal(String key, Map<String, Object> dataMap);
	abstract protected void putReal(Map<String,Map<String, Object>> keyValue);

	abstract protected boolean removeReal(String key);
	abstract protected void removeReal(Collection<String> keys);

	final protected String dealcache_uptime = "dealcache_uptime";
	final protected String dealcache_data = "data";
	final protected DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public <T, R> R get(T key, Function<T, R> getFunction) {
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

		Pair<R, LocalDateTime> objMap = null;

		if(cacheDefinition != null) {
			Map<String, Object> docMap = getReal(generateKey(key));
			if (EmptyUtil.isEmpty(docMap) == false) {
				objMap = parseData(docMap);
				hitCount++;
			}
		}

		// 추가
		if(getFunction != null) {
			if (objMap == null || objMap.getValue0() == null) {
				R r = getFunction.apply(key);
				if (r != null) {
					// 캐시에 추가
					put(generateKey(key), r);
				}

				objMap = new Pair<>(r, LocalDateTime.now());
			}

			// async cache time 이 만료된 상태일 경우
			addAsyncCacheProc(key, objMap.getValue1(), key2 -> {
				R r2 = getFunction.apply(key2);
				put(generateKey(key2), r2);
				return r2;
			});
		}

		getCount++;
		return objMap != null ? objMap.getValue0() : null;
	}


	@SuppressWarnings("unchecked")
	public <T, R> Map<T, R> gets(Collection<T> keys, Function<Collection<T>, Map<T, R>> getFunction) {
		mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

		Map<T, R> retMap = new HashMap<>();

		// generate된 key를 parse하고 난 결과물은 String으로만 처리가 가능하므로, 이를 다시 원래 형태로 되돌릴 방법이 없다.
		// 그래서 미리 key와 key.toString()간의 관계를 map으로 저장해두고, 향후 이를 참조하도록 하자
		Map<String, T> keyDic = keys.stream().collect(Collectors.toMap(key -> key.toString(), key -> key));

		if(cacheDefinition != null) {
			try {
				List<String> keyStr = keys.stream().map(this::generateKey).collect(Collectors.toList());
				Map<String, Map<String, Object>> ret = getReal(keyStr);

				ret.forEach((key, value) -> {
					Pair<R, LocalDateTime> objMap = parseData(value);

					// async cache time 이 만료된 상태일 경우
					addAsyncCacheProc(keyDic.get(parseKey(key)), objMap.getValue1(), key2 -> {
						Map<T, R> retMap2 = getFunction.apply(Arrays.asList(key2));
						if (EmptyUtil.isEmpty(retMap2) == false) {
							R newR = retMap2.get(key2);
							put(key2, newR);

							return newR;
						}

						return null;
					});

					retMap.put(keyDic.get(parseKey(key)), (R)objMap.getValue0());
				});
				hitCount += ret.size();
			} catch (Exception e) {
				log.error(this.getClass().getCanonicalName() + ".get : " + e.toString());
			}
		}

		if(getFunction != null) {
			// 없는 항목들을 모은다.
			Collection<T> newKeys = keys.stream()
					.filter(key -> !retMap.containsKey(key))
					.collect(Collectors.toList());

			if(EmptyUtil.collectionEmpty(newKeys) == false) {
				Map<T, R> retNewMap = getFunction.apply(newKeys);
				if (!EmptyUtil.isEmpty(retNewMap)) {
					// 캐시에 새로 추가
					puts(retNewMap);

					// retMap에도 추가
					retMap.putAll(retNewMap);
				}
			}

			// 있는 항목들 중
		}

		getCount += retMap.size();
		return retMap;
	}

	/**
	 * 비동기 캐시 조건을 체크하고 적용하는 함수
	 * addFunc 내부에서도 캐시타임을 다시 한번 조사하여, 그 사이에 이미 갱신되었으면 새로 굽지 않도록 확인한다.
	 *
	 * @param key
	 * @param rTime
	 * @param addFunc
	 * @param <T>
	 */
	protected <T, R> void addAsyncCacheProc(T key, LocalDateTime rTime, Function<T, R> addFunc) {
		if (remoteCacheAsyncTime > 0 && esAsyncCacheExecutor != null) {
			if(rTime == null || rTime.isBefore(LocalDateTime.now().minusSeconds(remoteCacheAsyncTime))) {
				// 비동기로 cache update
				log.info(String.format("###### Async cache update 요청 (%s) ######", key.toString()));
				esAsyncCacheExecutor.submit(() -> {
					// 지금 시간을 다시 확인한다.
					Map<String, Object> newData = getReal(generateKey(key));
					if(newData != null) {
						Pair<R, LocalDateTime> newParsedData = parseData(newData);
						if(newParsedData.getValue1().isAfter(LocalDateTime.now().minusSeconds(remoteCacheAsyncTime))) {
							log.info(String.format("###### Async cache update 이미 갱신완료 (%s) ######", key.toString()));
						}
					}

					R newR = addFunc.apply(key);
					if(localCacheClientToSync != null && newR != null) {
						localCacheClientToSync.put(key, newR);
						log.info(String.format("###### Async cache update 로컬캐시 동기화 (%s) ######", key.toString()));
					}
					log.info(String.format("###### Async cache update 갱신완료 (%s) ######", key.toString()));
				});
			}
		}
	}

	public <T, R> void put(T key, R value) {
		if(cacheDefinition != null) {
			if (value != null) {
				putReal(generateKey(key), genData(value));
			}
		}
	}

	public <T, R> void puts(Map<T, R> keyValue) {
		if(cacheDefinition != null) {
			// null 걸러내기
			Map<String, Map<String, Object>> keyValue2 = keyValue.entrySet().stream()
					.filter(entry -> entry.getValue() != null)
					.collect(Collectors.toMap(entry -> generateKey(entry.getKey()), entry -> genData(entry.getValue())));

			putReal(keyValue2);
		}
	}

	public <T> boolean remove(T key) {
		if(cacheDefinition != null) {
			return removeReal(generateKey(key));
		}

		return false;
	}

	public <T> void removes(Collection<T> keys) {
		if(cacheDefinition != null) {
			removeReal(generateKeys(keys));
		}
	}

	private <T> String generateKey(T key) {
		return String.format("%s::%s", cacheDefinition != null ? cacheDefinition.getName() : "", key.toString());
	}

	private <T> Collection<String> generateKeys(Collection<T> keys) {
		return keys.stream().map(this::generateKey).collect(Collectors.toList());
	}

	private String parseKey(String keyStr) {
		return keyStr.replaceAll(cacheDefinition != null ? (cacheDefinition.getName() + "::") : "", "");
	}

	private <T> Map<String, Object> genData(T value) {
		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put(dealcache_data, value);
		dataMap.put(dealcache_uptime, now());

		return dataMap;
	}

	@SuppressWarnings("unchecked")
	private <R> Pair<R, LocalDateTime> parseData(Map<String, Object> dataMap) {
		LocalDateTime t = dataMap.containsKey(dealcache_uptime) ?  time(dataMap.get(dealcache_uptime).toString()) : null;

		R r = null;
		if(dataMap.containsKey(dealcache_data)) {
			Object obj = dataMap.get(dealcache_data);
			if(obj instanceof HashMap) {
				try {
					r = mapper.convertValue(obj, (Class<R>)cacheDefinition.getCacheModelClass());
				} catch(Exception e) {

				}
			} else {
				r = (R)obj;
			}
		}

		return new Pair<>(r, t);
	}

	private LocalDateTime parseUptime(Map<String, Object> dataMap) {
		return dataMap.containsKey(dealcache_uptime) ?  time(dataMap.get(dealcache_uptime).toString()) : null;
	}

	protected String now() {
		return LocalDateTime.now().format(dtFormatter);
	}

	protected LocalDateTime time(String t) {
		return LocalDateTime.parse(t, dtFormatter);
	}
}
