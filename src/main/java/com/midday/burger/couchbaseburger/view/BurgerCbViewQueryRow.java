package com.midday.burger.couchbaseburger.view;

import java.io.IOException;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.AsyncViewRow;
import rx.Observable;

/**
 * Couchbase View Query 결과
 *
 * key/value/document를 가진다.
 * value는 null일 수 있다.
 * document는 비동기적으로 parsing하며, getDocument()에서 blocking된다. 한번 parse되고 난 후에는 blocking되지 않는다.
 *
 * @author midday
 */

public class BurgerCbViewQueryRow {
	private String id;
	private Object key = null;
	private Object value = null;

private Observable<JsonDocument> document = null;

	private ObjectMapper mapper = new ObjectMapper();

	protected BurgerCbViewQueryRow() {
		mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	public static BurgerCbViewQueryRow createNew(AsyncViewRow row) {
		BurgerCbViewQueryRow qvRow = new BurgerCbViewQueryRow();

		qvRow.id = row.id();
		qvRow.key = row.key();
		qvRow.value = row.value();
		qvRow.document = row.document();

		return qvRow;
	}

	public <T> T getValue(Class<T> toValueType) throws IOException {
		if (isValueNull() == true) {
			return null;
		} else if (isValueMap() == false) {
			return (T) value;
		}

		return mapper.readValue(JsonObject.from(((JsonObject) value).toMap()).toString(), toValueType);
	}

	public <T> Observable<T> getDocument(Class<T> toValueType) throws Exception {
		if(document == null) return null;

		return this.document.map(jsonDoc -> {
					try {
						return mapper.readValue(jsonDoc.content().toString(), toValueType);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				});
	}

	public String getId() {
		return id;
	}

	public Object getKey() {
		return key;
	}

	public Object getValue() {
		return value;
	}

	public Observable<JsonDocument> getDocument() {
		return document;
	}

	public Boolean isKeyArray() {
		return (key instanceof JsonArray);
	}

	public Boolean isValueArray() {
		return (value != null && (value instanceof JsonArray));
	}

	public Boolean isValueNull() {
		return (value == null);
	}

	public Boolean isValueMap() {
		return (value instanceof JsonObject);
	}
}
