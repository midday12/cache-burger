package com.midday.burger.couchbaseburger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.couchbase.client.core.time.Delay;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.error.CASMismatchException;
import com.couchbase.client.java.error.TemporaryLockFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;
import com.couchbase.client.java.util.retry.RetryWhenFunction;
import com.midday.burger.couchbaseburger.view.BurgerCbViewQuery;
import com.midday.burger.couchbaseburger.view.BurgerCbViewQueryRow;
import com.midday.burger.couchbaseburger.view.BurgerCbViewQueryStale;
import lombok.Getter;
import lombok.Setter;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.deps.io.netty.util.ResourceLeakDetector;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DocumentDoesNotExistException;
import com.couchbase.client.java.view.ViewQuery;

import org.javatuples.Pair;
import org.springframework.beans.factory.DisposableBean;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.MathObservable;

/**
 * TPIN Query를 Wrapping하는 Helper 클래스
 *
 * @author midday
 */

public class BurgerCbDatabaseClient implements DisposableBean {
	private ObjectMapper mapper = new ObjectMapper();

	static private CouchbaseCluster cbCluster = null;

	private Bucket cbBucket = null;

	private String serverNodes = "";
	private String bucketName = "";
	private String bucketPwd = "";

	private int timeOutMilliseconds = 5 * 1000;
	private int retryDelay = 10;

	private int timeOutMillisecondsBackpressure = 2 * 1000;
	private int retryDelayBackPressure = 100;

	@Getter @Setter
	private boolean retryWhenBackpressure = true;

	private BurgerCbWaitLockMode waitLockMode = BurgerCbWaitLockMode.LOCK_WAIT;

	public BurgerCbDatabaseClient(String nodes, String bucket, String pwd) {
		serverNodes = nodes;
		bucketName = bucket;
		bucketPwd = pwd;

		connect();
	}

	public BurgerCbDatabaseClient(String nodes, String bucket, String pwd, String resourceLeakDetectorLevel) {
		serverNodes = nodes;
		bucketName = bucket;
		bucketPwd = pwd;

		if (resourceLeakDetectorLevel.isEmpty() == false) {
			ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.valueOf(resourceLeakDetectorLevel));
		}

		connect();
	}

	@Override
	public void destroy() throws Exception {
		disconnect();
	}

	public Boolean connect() {
		try {
			if (cbCluster == null) {
				CouchbaseEnvironment env = DefaultCouchbaseEnvironment
				        .builder()
				        //.viewEndpoints(10)
				        .build();

				String[] nodeArray = new String(serverNodes).split(",");
				cbCluster = CouchbaseCluster.create(env, Arrays.asList(nodeArray));
			}

			cbBucket = cbCluster.openBucket(bucketName, bucketPwd, timeOutMilliseconds, TimeUnit.MILLISECONDS);
			//} catch (InvalidPasswordException e) {
			//	openBucketException = new InvalidPasswordException(e.toString() + " [Node : " + nodes + ", Bucket : " + bucket + ", Password : " + pwd + "]", e);
			//	openBucketException.printStackTrace(System.err);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}

		return isConnected();
	}

	public void disconnect() {
		try {
			cbBucket.close();
		} catch (Exception e) {
			System.err.println(e);
		}

		serverNodes = "";
		bucketName = "";
		bucketPwd = "";
	}

	public Boolean isConnected() {
		return (cbCluster != null);
	}

	public Bucket getBucket() {
		return cbBucket;
	}

	public AsyncBucket getAsyncBucket() {
		return cbBucket.async();
	}

	public BurgerCbWaitLockMode getWaitLockMode() {
		return waitLockMode;
	}

	public void setWaitLockMode(BurgerCbWaitLockMode waitLockMode) {
		this.waitLockMode = waitLockMode;
	}

	/**
	 * DB 함수를 호출할 때 사용하는 Timeout 값. Millisecond 단위
	 *
	 * @return Millisecond 단위
	 */
	public int getTimeOutMilliseconds() {
		return timeOutMilliseconds;
	}

	/**
	 * DB 함수를 호출할 때 사용하는 Timeout 값. Millisecond 단위이며, 0을 입력할 경우 기본값은 30000.
	 *
	 * @param timeOutMilliseconds
	 * @return
	 */
	public void setTimeOutMilliseconds(int timeOutMilliseconds) {
		if (timeOutMilliseconds == 0) {
			this.timeOutMilliseconds = 30 * 1000;
		} else {
			this.timeOutMilliseconds = timeOutMilliseconds;
		}
	}

	public Boolean checkConnection() throws Exception {
		if (getBucket() == null) {
			throw new NullPointerException("Couchbase bucket is null");
		} else if (isConnected() == false) {
			connect();
		}

		return true;
	}

	/**
	 * Couchbase로부터 Document 존재여부를 확인한다.
	 *
	 * @param id // Couchbase의 ID
	 * @return Observable<Boolean>
	 * @throws Exception
	 */
	public Observable<Boolean> exist(String id) throws Exception {
		checkConnection();

		return getAsyncBucket().exists(id).timeout((long)timeOutMilliseconds, TimeUnit.MILLISECONDS, rx.Observable.just(Boolean.FALSE))
				.retryWhen(retryWhenBackpressure());
	}

	/**
	 * Couchbase로부터 1개의 Record를 읽어온다.
	 *
	 * @param id // Couchbase의 ID
	 * @return Observable<BurgerCbDocument>
	 * @throws Exception
	 */
	public Observable<BurgerCbDocument> get(String id) throws Exception {
		return get(id, 0);
	}

	/**
	 * Couchbase로부터 1개의 Record를 읽어온다.
	 *
	 * @param id // Couchbase의 ID
	 * @param lockSecond : lock time. 0일 경우 no lock. 30초가 넘을 경우 기본값 15초.
	 * @return Observable<BurgerCbDocument>
	 * @throws Exception
	 */
	public Observable<BurgerCbDocument> get(String id, int lockSecond) throws Exception {
		checkConnection();

		Observable<BurgerCbDocument> queryObj = null;

		try {
			Observable<JsonDocument> obsJsonDoc = (lockSecond == 0) ? getAsyncBucket().get(id) :  getAsyncBucket().getAndLock(id, lockSecond);

			queryObj = obsJsonDoc.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
					.retryWhen(retryWhenBackpressure())
					.retryWhen(retryGetLockMode)
					.filter(jsonDoc -> jsonDoc != null && jsonDoc.id().equals(id))
					.map(jsonDoc -> new BurgerCbDocument(jsonDoc.id(), jsonDoc.content().toString(), jsonDoc.cas()));
		} catch (NoSuchElementException e) {
			queryObj = Observable.just(null);
		}

		return queryObj;
	}

	/**
	 * Couchbase로부터 1개의 Record를 읽어온다.
	 *
	 * @param id // Couchbase의 ID
	 * @return <T> T
	 * @throws Exception
	 */
	public <T> Observable<T> get(String id, Class<T> toValueType) throws Exception {
		checkConnection();

		Observable<BurgerCbDocument> docObs = get(id);
		if (docObs == null) return null;

		return docObs.map(doc -> {
			try {
				return BurgerCbDocument.to(doc.getPropertyMap(), toValueType);
			} catch(IOException e) {
				throw new RuntimeException(e);
			}
		});
	}

	/**
	 * JSON 형태가 아닌 단순 문자열로 구성된 Document를 읽어올 수 있다.
	 *
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public Observable<String> getString(String id) throws Exception {
		checkConnection();

		Observable<StringDocument> strDocResultObs = null;

		try {
			strDocResultObs = getAsyncBucket().get(StringDocument.create(id))
					.retryWhen(retryWhenBackpressure())
			        .timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null));
		} catch (NoSuchElementException e) {
			strDocResultObs = Observable.just(null);
		}

		if (strDocResultObs != null) {
			return strDocResultObs.map(strDocResult -> strDocResult.content().toString());
		} else {
			return Observable.just("");
		}
	}

	/**
	 * Couchbase로부터 n개의 Record를 읽어온다.
	 *
	 * @param ids // Couchbase의 ID 목록
	 * @return Map<String, TPINQueryObject> (<ID, TPINQueryObject>, Exception이 발생하였을 경우 null을 리턴)
	 * @throws Exception
	 */
	public <T>  Observable<Map<String, T>> gets(List<String> IDs, Class<T> toValueType) throws Exception {
		checkConnection();

		return gets(IDs)
				.flatMap(idMap -> Observable.from(idMap.entrySet()))
				.flatMap(idEntry -> {
					try {
						return Observable.just(new Pair<>(idEntry.getKey(), idEntry.getValue().to(toValueType)));
					} catch(Exception e) {
						e.printStackTrace(System.err);
					}

					return Observable.just(null);
				})
				.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
				.retryWhen(retryWhenBackpressure())
				.filter(idPair -> idPair != null)
				.toMap(idPair -> idPair.getValue0(), idPair -> idPair.getValue1());
	}

	/**
	 * @return Map<String, TPINDocument> (<ID, TPINDocument>>, 읽어오는데 성공한 Document의 목록)
	 * @throws Exception Couchbase로부터 n개의 Record를 읽어온다.
	 * 내부적인 로직이 구현된 private function
	 * @throws
	 */
	public Observable<Map<String, BurgerCbDocument>> gets(List<String> IDs) throws Exception {
		checkConnection();

		return Observable.from(IDs)
				.flatMap(id -> {
					try {
						return get(id);
					} catch (Exception e) {
						e.printStackTrace(System.err);
					}

					return Observable.just(null);
				})
				.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
				.retryWhen(retryWhenBackpressure())
				.filter(jsonDoc -> jsonDoc != null)
				.filter(jsonDoc -> jsonDoc.getID().isEmpty() == false)
				.toMap(jsonDoc -> jsonDoc.getID(), jsonDoc -> jsonDoc);
	}

	private ViewQuery toViewQuery(BurgerCbViewQuery viewObj) {
		if (viewObj.getViewName().isEmpty() == true || viewObj.getDocName().isEmpty() == true) {
			return null;
		}

		ViewQuery viewQuery = ViewQuery.from(viewObj.getDocName(), viewObj.getViewName());

		viewQuery.debug(viewObj.getDebugView());
		viewQuery.reduce(viewObj.getDoReduce());
		viewQuery.descending(viewObj.getDescendant());

		viewQuery.stale(viewObj.getStale().getStale());

		if (viewObj.getKey().isEmpty() == false) {
			if (viewObj.getKey().size() == 1) {
				Object objKey = viewObj.getKey().get(0);

				if (objKey instanceof Long) {
					viewQuery.key((Long) objKey);
				} else if (objKey instanceof Integer) {
					viewQuery.key((Integer) objKey);
				} else if (objKey instanceof Double) {
					viewQuery.key((Double) objKey);
				} else if (objKey instanceof Boolean) {
					viewQuery.key((Boolean) objKey);
				} else if (objKey instanceof String) {
					viewQuery.key(objKey.toString());
				}
			} else {
				viewQuery.keys(JsonArray.from(viewObj.getKey()));
			}
		} else {
			if (viewObj.getStartKey().isEmpty() == false) {
				if (viewObj.getStartKey().size() == 1) {
					Object objKey = viewObj.getStartKey().get(0);

					if (objKey instanceof Long) {
						viewQuery.startKey((Long) objKey);
					} else if (objKey instanceof Integer) {
						viewQuery.startKey((Integer) objKey);
					} else if (objKey instanceof Double) {
						viewQuery.startKey((Double) objKey);
					} else if (objKey instanceof Boolean) {
						viewQuery.startKey((Boolean) objKey);
					} else if (objKey instanceof String) {
						viewQuery.startKey(objKey.toString());
					}
				} else {
					viewQuery.startKey(JsonArray.from(viewObj.getStartKey()));
				}
			}

			if (viewObj.getEndKey().isEmpty() == false) {
				if (viewObj.getEndKey().size() == 1) {
					Object objKey = viewObj.getEndKey().get(0);

					if (objKey instanceof Long) {
						viewQuery.endKey((Long) objKey);
					} else if (objKey instanceof Integer) {
						viewQuery.endKey((Integer) objKey);
					} else if (objKey instanceof Double) {
						viewQuery.endKey((Double) objKey);
					} else if (objKey instanceof Boolean) {
						viewQuery.endKey((Boolean) objKey);
					} else if (objKey instanceof String) {
						viewQuery.endKey(objKey.toString());
					}
				} else {
					viewQuery.endKey(JsonArray.from(viewObj.getEndKey()));
				}
			}
		}

		if (viewObj.getStartKeyDocId().isEmpty() == false) {
			viewQuery.startKeyDocId(viewObj.getStartKeyDocId());
		}

		if (viewObj.getEndKeyDocId().isEmpty() == false) {
			viewQuery.endKeyDocId(viewObj.getEndKeyDocId());
		}

		if (viewObj.getPageNumber() >= 0 && viewObj.getPageLimit() > 0) {
			viewQuery.limit(viewObj.getPageLimit());
			viewQuery.skip(viewObj.getPageNumber() * viewObj.getPageLimit());
		}

		if (viewObj.getGroupLevel() > 0) {
			viewQuery.groupLevel(viewObj.getGroupLevel());
			viewObj.setGroup(false);
		}

		if (viewObj.getGroup() == true) {
			viewQuery.group(true);
		}

		viewQuery.inclusiveEnd(viewObj.getInclusiveEnd());
		viewQuery.includeDocs(viewObj.getIncludeDocs());

		return viewQuery;
	}

	/**
	 * Couchbase로부터 View Query를 실행함
	 *
	 * @param viewObj
	 * @return List<TPINQueryViewRow> (Query 결과 목록)
	 * @throws Exception
	 */
	public Observable<List<BurgerCbViewQueryRow>> query(BurgerCbViewQuery viewObj) throws Exception {
		checkConnection();

		try {
			return getAsyncBucket().query(toViewQuery(viewObj))
			        //.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
					.retryWhen(retryWhenBackpressure())
			        .flatMap(t1 -> (t1 != null ? t1.rows() : Observable.just(null)))
			        .filter(row -> (row != null))
			        .flatMap(row -> Observable.just(BurgerCbViewQueryRow.createNew(row)))
					.toList();
		} catch (BackpressureException be) {
			System.out.println("Query BackpressureException : " + viewObj.toString());

			try {
				Thread.sleep(10);
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		} catch (Exception e) {
			throw e;
		}

		return Observable.just(Collections.EMPTY_LIST);
	}

	/**
	 * Couchbase로부터 View Query를 실행하여 Value를 T로 변환
	 *
	 * @param viewObj
	 * @param toValueType
	 * @return List<T> (Query 결과 목록)
	 * @throws Exception
	 */
	public <T> Observable<List<T>> query(BurgerCbViewQuery viewObj, Class<T> toValueType) throws Exception {
		checkConnection();

		return query(viewObj)
				.retryWhen(retryWhenBackpressure())
				.flatMap(Observable::from)
				.flatMap(BurgerCbViewQueryRow::getDocument)
				.map(doc -> {
					try {
						return mapper.readValue(doc.content().toString(), toValueType);
					} catch (Exception e) {
						e.printStackTrace(System.err);
						return null;
					}
				})
				.filter(t -> t != null)
				.toList();
	}

	/**
	 * Couchbase로부터 View Query를 실행함. 단, reduce()를 반드시 실행하고 결과를 long으로 받아오는 경우에만 사용할 수 있음
	 *
	 * @param viewObj
	 * @return long
	 * @throws Exception
	 */
	public Observable<Long> queryCount(BurgerCbViewQuery viewObj) throws Exception {
		checkConnection();

		viewObj.setDoReduce(true);

		try {
			Observable<Long> sumObs = getAsyncBucket().query(toViewQuery(viewObj))
					.retryWhen(retryWhenBackpressure())
			        .flatMap(t1 -> (t1 != null ? t1.rows() : Observable.just(null)))
			        .filter(row -> (row != null))
			        .flatMap(row -> Observable.just(Long.parseLong(row.value().toString())));

			return MathObservable.sumLong(sumObs);
		} catch (BackpressureException be) {
			System.out.println("Query BackpressureException : " + viewObj.toString());

			try {
				Thread.sleep(10);
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		} catch (Exception e) {
			throw e;
		}

		return Observable.just(0L);
	}

	/**
	 * Couchbase로부터 View Query를 실행함. 단, reduce()를 반드시 실행하고 결과를 long으로 받아오는 경우에만 사용할 수 있음
	 *
	 * @param query
	 * @param stale
	 * @throws Exception
	 */
	public Observable<Long> queryCount(String bucketName, String viewName, BurgerCbViewQueryStale stale) throws Exception {
		checkConnection();

		BurgerCbViewQuery cvQuery = new BurgerCbViewQuery(bucketName, viewName).setStale(stale);
		return queryCount(cvQuery);
	}

	/**
	 * Couchbase의 View Query를 stale=false로 호출하여 index를 최신정보로 flush해준다.
	 *
	 * @param query
	 * @throws Exception
	 */
	public void queryFlush(String bucketName, String viewName) throws Exception {
		checkConnection();

		BurgerCbViewQuery cvQuery = new BurgerCbViewQuery(bucketName, viewName);

		cvQuery.setStale(BurgerCbViewQueryStale.STALE_FALSE);
		cvQuery.setPageLimit(1);
		cvQuery.setPageNumber(0);

		query(cvQuery);
	}

	/**
	 * Couchbase로 하나의 Document를 저장함
	 *
	 * @param id (Document ID)
	 * @param t (Java Class, ObjectMapper로 변환됨)
	 * @param toValueType (변환에 사용될 class type)
	 * @param expiry (the expiration time of the document). 0일 경우 무시
	 * @return String (저장된 Document의 ID)
	 * @throws Exception
	 */
	public <T> Observable<String> put(String id, T t, Class<T> toValueType, int expiry) throws Exception {
		checkConnection();

		return put(BurgerCbDocument.to(id, t, toValueType), expiry).flatMap(cbDoc -> Observable.just(cbDoc.getID()));
	}

	/**
	 * Couchbase로 하나의 Document를 저장함
	 *
	 * @param cbDoc
	 * @param expiry (the expiration time of the document). 0일 경우 무시
	 * @return String (저장된 Document의 ID)
	 * @throws Exception("DocumentAlreadExists")
	 */
	public Observable<BurgerCbDocument> put(BurgerCbDocument cbDoc, int expiry) throws Exception {
		checkConnection();

		Observable.just(null);

		Observable<JsonDocument> jsonDocObs = getAsyncBucket().upsert(JsonDocument.create(cbDoc.getID(), expiry, JsonObject.fromJson(cbDoc.getJsonStr()), cbDoc.getCas()))
				.retryWhen(retryWhenBackpressure())
				.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
				.retryWhen(retryUpdateLockMode);

		return jsonDocObs.flatMap(jsonDoc -> Observable.just(new BurgerCbDocument(jsonDoc.id(), jsonDoc.content().toMap(), jsonDoc.cas())));
	}

	/**
	 * Couchbase로 여러개의 Document를 저장함
	 * 내부적인 로직이 구현된 private function
	 *
	 * @param expiry (the expiration time of the document). 0일 경우 무시
	 * @param tmonDocList (저장할 Document 목록)
	 * @return List<String> (저장된 Document의 ID List)
	 * @throws Exception
	 */
	public Observable<List<String>> puts(int expiry, List<BurgerCbDocument> tmonDocList) throws Exception {
		checkConnection();

		return Observable.from(tmonDocList)
				.flatMap(doc -> {
					try {
						return put(doc, expiry);
					} catch(Exception e) {
						return rx.Observable.error(e);
					}
				})
				.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
				.filter(doc -> doc.getID().isEmpty() == false)
				.flatMap(doc -> Observable.just(doc.getID()))
				.toList();
	}

	/**
	 * @param jsonDocs (저장할 Document 목록)
	 * @param expiry (the expiration time of the document). 0일 경우 무시
	 * @return List<String> (저장된 Document의 ID List)
	 * @throws Exception Couchbase로 여러개의 Document를 저장함
	 * 내부적인 로직이 구현된 private function
	 * @throws
	 */
	public Observable<List<String>> puts(int expiry, BurgerCbDocument... jsonDocs) throws Exception {
		checkConnection();

		return puts(expiry, Arrays.asList(jsonDocs));
	}

	/**
	 * 단순한 String 타입의 document 저장
	 *
	 * @param id
	 * @param content
	 * @param expiry (the expiration time of the document). 0일 경우 무시
	 * @return
	 * @throws Exception
	 */
	public Observable<Boolean> putString(String id, String content, int expiry) throws Exception {
		checkConnection();

		StringDocument strDoc = StringDocument.create(id, expiry, content);

		/**
		 * 참고하세요
		 *
		 * // insert : 추가. 이미 동일한 ID가 있을 경우 에러
		 * JsonDocument newJsonDoc = bucket.insert(jsonDoc);
		 *
		 * // upsert : 추가. 이미 동일한 ID가 있을 경우 덮어씀
		 * JsonDocument newJsonDoc = bucket.upsert(jsonDoc);
		 *
		 * // append : 추가. 이미 동일한 ID가 있을 경우, 같은 ID로 또 추가
		 * JsonDocument newJsonDoc = bucket.append(jsonDoc);
		 *
		 */

		return getAsyncBucket().upsert(strDoc)
				.retryWhen(retryUpdateLockMode)
				.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
				.map(jsonDoc -> (jsonDoc != null && jsonDoc.id().isEmpty() == false));
	}

	/**
	 * Couchbase의 Document를 삭제함
	 *
	 * @param id (Document ID)
	 * @return Boolean (삭제 성공 여부)
	 * @throws Exception
	 */
	public Observable<Boolean> remove(String id) throws Exception {
		checkConnection();

		try {
			return getAsyncBucket().remove(id)
					.retryWhen(retryWhenBackpressure())
					.timeout(timeOutMilliseconds, TimeUnit.MILLISECONDS, Observable.just(null))
					.map(jsonDoc -> (jsonDoc != null && jsonDoc.id().isEmpty() == false))
					.retryWhen(retryUpdateLockMode);
		} catch (DocumentDoesNotExistException e1) { // Document가 없으면 어차피 삭제된거나 마찬가지임
			return Observable.just(true);
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}

		return Observable.just(false);
	}

	/**
	 * Couchbase의 여러개의 Document를 삭제함
	 *
	 * @param IDs (Document ID 목록)
	 * @return @return List<String> (삭제 성공한 Document의 ID 목록)
	 * @throws Exception
	 */
	public Observable<Integer> removes(String... IDs) throws Exception {
		checkConnection();

		return removes(Arrays.asList(IDs));
	}

	/**
	 * Couchbase의 여러개의 Document를 삭제함
	 *
	 * @param IDs (Document ID 목록)
	 * @return List<String> (삭제 성공한 Document의 ID 목록)
	 * @throws Exception
	 */
	public Observable<Integer> removes(List<String> IDs) throws Exception {
		checkConnection();

		return Observable.from(IDs)
				.flatMap(id -> {
					try {
						return remove(id);
					} catch(Exception e) {
						return Observable.error(e);
					}
				})
				.filter(ret -> ret)
				.count();
	}

	/**
	 * Couchbase의 Atomic Counter 기능.
	 * http://docs.couchbase.com/developer/java-2.0/documents-atomic.html
	 *
	 * @param ID (the id of the document)
	 * @param delta (delta the increment or decrement amount)
	 * @return String (결과 ID 문자열)
	 * @throws Exception
	 */
	public String atomicCounterFunction(String ID, long delta) throws Exception {
		checkConnection();

		String result = new String();

		result = getBucket().counter(ID, delta, timeOutMilliseconds, TimeUnit.MILLISECONDS).content().toString();

		return result;
	}

	/**
	 * Couchbase의 bucket flush 기능.
	 *
	 * @return Boolean
	 * @throws Exception
	 */
	public Boolean flushBucket() throws Exception {
		checkConnection();

		BucketManager bucketManager = getBucket().bucketManager();
		if (bucketManager != null) {
			return bucketManager.flush(timeOutMilliseconds, TimeUnit.MILLISECONDS);
		}

		return false;
	}

	/**
	 * Blocking 모드로 간편하게 사용할 수 있는 exist 기능을 지원
	 *
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public boolean simpleExist(String id) throws Exception {
		return exist(id).toBlocking().single();
	}

	/**
	 * Blocking 모드로 간편하게 사용할 수 있는 get 기능을 지원
	 *
	 * @param id
	 * @param valueType
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public <T> T simpleGet(String id, Class<T> valueType) throws Exception {
		return get(id, valueType).toBlocking().single();
	}

	/**
	 * Blocking 모드로 간편하게 사용할 수 있는 gets 기능을 지원
	 *
	 * @param ids
	 * @param valueType
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public <T> Map<String, T> simpleGets(List<String> ids, Class<T> valueType) throws Exception {
		return gets(ids, valueType).toBlocking().single();
	}

	/**
	 * Blocking 모드로 간편하게 사용할 수 있는 getString 기능을 지원
	 *
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public String simpleGetString(String id) throws Exception {
		return getString(id).toBlocking().single();
	}

	/**
	 * Blocking 모드로 간편하게 사용할 수 있는 put 기능을 지원
	 *
	 * @param id
	 * @param t
	 * @param toValueType
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public <T> boolean simplePut(String id, T t,  Class<T> toValueType) throws Exception {
		return put(id, t, toValueType, 0).toBlocking().single().equals(id);
	}

	/**
	 * Blocking 모드로 간편하게 사용할 수 있는 putString 기능을 지원
	 *
	 * @param id
	 * @param content
	 * @param <T>
	 * @return
	 * @throws Exception
	 */
	public boolean simplePutString(String id, String content) throws Exception {
		return putString(id, content, 0).toBlocking().single();
	}

	public int simpleRemoves(String... IDs) throws Exception {
		return removes(IDs).toBlocking().single();
	}

	public Observable<Boolean> unlock(String id, long cas) {
		return getAsyncBucket().unlock(id, cas);
	}

	/**
	 * Backpressure Exception 발생 시 사용할 수 있는 RetryBuild
	 */
	protected RetryWhenFunction retryWhenBackpressure() {
		return RetryBuilder.anyOf(BackpressureException.class)
			.delay(Delay.exponential(TimeUnit.MILLISECONDS, timeOutMillisecondsBackpressure))
			.max(timeOutMillisecondsBackpressure / retryDelayBackPressure)
			.build();
	}

	/**
	 * Lock 상태에서 get 시 LOCK_WAIT를 걸기 위해 retryWhen 안에서 참조되는 함수
	 */
	protected Func1<? super Observable<? extends Throwable>, ? extends Observable<?>> retryGetLockMode = (t) -> {
		return t.flatMap(e -> {
			if (waitLockMode == BurgerCbWaitLockMode.LOCK_WAIT) {
				if(e instanceof TemporaryLockFailureException) {
					return rx.Observable.timer(retryDelay, TimeUnit.MILLISECONDS);
				}
			}

			return rx.Observable.error(e);
		});
	};

	/**
	 * Lock 상태에서 update, remove 시 LOCK_WAIT를 걸기 위해 retryWhen 안에서 참조되는 함수
	 */
	protected Func1<? super Observable<? extends Throwable>, ? extends Observable<?>> retryUpdateLockMode = (t) -> {
		return t.flatMap(e -> {
			if (waitLockMode == BurgerCbWaitLockMode.LOCK_WAIT) {
				if(e instanceof CASMismatchException) {
					return rx.Observable.timer(retryDelay, TimeUnit.MILLISECONDS);
				}
			}

			return rx.Observable.error(e);
		});
	};
}
