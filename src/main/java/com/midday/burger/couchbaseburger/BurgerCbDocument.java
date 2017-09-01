package com.midday.burger.couchbaseburger;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.couchbase.client.deps.com.fasterxml.jackson.core.JsonParseException;
import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.DeserializationFeature;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.JsonMappingException;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.document.json.JsonObject;

/**
 * Couchbase에서 사용되는 POJO 객체로 바꿔주기 위한 중간 역할을 하는 Document 객체
 *
 * @author midday
 */

public class BurgerCbDocument {
	/**
	 * Document ID
	 */
	private String ID = "";

	/**
	 * Document Json - Document Object Map과 동일한 데이터를 가지고 있음
	 */
	private String jsonStr = "";

	/**
	 * Document Object Map - Document Json과 동일한 데이터를 가지고 있음
	 */
	private Map<String, Object> jsonObjMap = null;

	/**
	 * getAndLock()을 통해 locking mode일 때 cas value
	 * unlock()에서 사용함
	 */
	private long cas = 0;

	private ObjectMapper mapper = new ObjectMapper();

	/**
	 * 생성자
	 *
	 * @param
	 * @return
	 */
	public BurgerCbDocument() {
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * 생성자
	 *
	 * @param ID(document id), json(document)
	 * @return
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public BurgerCbDocument(String ID, String json) {
		this.ID = ID;
		setJsonStr(json);
	}

	public BurgerCbDocument(String ID, String json, long cas) {
		this.ID = ID;
		this.cas = cas;
		setJsonStr(json);
	}

	public BurgerCbDocument(String ID, Map<String, Object> objsMap) {
		this.ID = ID;
		setPropertyMap(objsMap);
	}

	public BurgerCbDocument(String ID, Map<String, Object> objsMap, long cas) {
		this.ID = ID;
		this.cas = cas;
		setPropertyMap(objsMap);
	}

	public String getID() {
		return ID;
	}

	public void setID(String ID) {
		this.ID = ID;
	}

	public String getJsonStr() {
		return jsonStr;
	}

	public long getCas() { return cas; }

	public void setCas(long cas) { this.cas = cas; }

	public Map<String, Object> getPropertyMap() {
		Map<String, Object> objCopy = jsonObjMap;
		return objCopy;
	}

	/**
	 * Json 문서를 지정. 지정된 Json 문서는 미리 Map 객체로 Parse해둔다.
	 *
	 * @param json
	 * @return
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonParseException
	 */
	public void setJsonStr(String json)  {
		this.jsonStr = json;

		if (jsonStr.isEmpty() == false) {
			try {
				jsonObjMap = mapper.readValue(json, new TypeReference<LinkedHashMap<String, Object>>() {
				});
			} catch(IOException e) {
				jsonObjMap = null;
			}
		} else {
			jsonObjMap = null;
		}
	}

	/**
	 * Object HashMap을 지정. Map 객체를 Json으로 미리 조합해둔다.
	 *
	 * @param Map<String, Object>
	 * @return
	 * @throws IOException
	 * @throws JsonParseException
	 */
	public void setPropertyMap(Map<String, Object> objsMap) {
		jsonObjMap = objsMap;

		if (objsMap.isEmpty() == false) {
			jsonStr = JsonObject.from(objsMap).toString();
		} else {
			jsonStr = "";
		}
	}

	/**
	 * ObjectMapper를 이용하여 POJO로 변환
	 *
	 * @param toValuetype
	 * @return T(Class Template)
	 * @exception JsonParseException, JsonMappingException, IOException
	 */
	public <T> T to(Class<T> toValueType) throws JsonParseException, JsonMappingException, IOException {
		return mapper.readValue(jsonStr, toValueType);
	}

	/**
	 * Json 문서를 ObjectMapper를 이용하여 POJO로 변환
	 *
	 * @param json, toValuetype
	 * @return T(Class Template)
	 * @exception JsonParseException, JsonMappingException, IOException
	 */
	public static <T> T to(String json, Class<T> toValueType) throws JsonParseException, JsonMappingException, IOException {
		T value = null;

		BurgerCbDocument tpinDocument = new BurgerCbDocument();
		value = tpinDocument.mapper.readValue(json, toValueType);

		return value;
	}

	/**
	 * Json Mao Object를 ObjectMapper를 이용하여 POJO로 변환
	 *
	 * @param objMap, toValuetype
	 * @return T(Class Template)
	 * @exception JsonParseException, JsonMappingException, IOException
	 */
	public static <T> T to(Map<String, Object> objMap, Class<T> toValueType) throws JsonParseException, JsonMappingException, IOException {
		T value = null;

		BurgerCbDocument tpinDocument = new BurgerCbDocument();
		value = tpinDocument.mapper.readValue(JsonObject.from(objMap).toString(), toValueType);

		return value;
	}

	/**
	 * POJO 를 ObjectMapper를 이용하여 Json 문서로 변환
	 *
	 * @param tpinInfo, toValuetype
	 * @return String(Json 문서)
	 * @exeption IllegalArgumentException, IOException
	 */
	public static <T> String to(Object tpinInfo) throws IllegalArgumentException, IOException {
		BurgerCbDocument tpinDocument = new BurgerCbDocument();
		String json = tpinDocument.mapper.writeValueAsString(tpinInfo);

		return json;
	}

	/**
	 * POJO 를 ObjectMapper를 이용하여 Map으로 변환
	 *
	 * @param tpinInfo, toValuetype
	 * @return Map<String, Object>
	 * @exeption IllegalArgumentException, IOException
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, Object> toMap(Object tpinInfo) throws IllegalArgumentException, IOException {
		BurgerCbDocument tpinDocument = new BurgerCbDocument();

		return tpinDocument.mapper.convertValue(tpinInfo, HashMap.class);
	}

	/**
	 * POJO를 ObjectMapper를 이용하여 TPINDocument로 변환
	 *
	 * @param json, toValuetype
	 * @return T(Class Template)
	 * @exception JsonParseException, JsonMappingException, IOException
	 */
	public static <T> BurgerCbDocument to(String ID, T obj, Class<T> fromValueType) throws JsonParseException, JsonMappingException, IOException {
		Map<String, Object> objMap = BurgerCbDocument.toMap(obj);

		return new BurgerCbDocument(ID, objMap);
	}

	/**
	 * POJO 에 지정되지 않은 Json Property에 접근하도록 해줌
	 *
	 * @param propName
	 * @return Object(Json Property)
	 */
	public Object getProperty(String propName) {
		if (jsonObjMap.containsKey(propName) == false) {
			return null;
		}

		return jsonObjMap.get(propName);
	}
}
