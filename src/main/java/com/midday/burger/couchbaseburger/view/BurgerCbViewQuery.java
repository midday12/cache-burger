package com.midday.burger.couchbaseburger.view;

import java.util.ArrayList;
import java.util.List;

/**
 * Couchbase의 View를 다루기 위한 Wrapping class
 *
 * @author midday
 */

public class BurgerCbViewQuery {
	private String docName = "";
	private String viewName = "";

	private Integer pageLimit = 0;
	private Integer pageNumber = 0;

	private Boolean debugView = false;

	private Boolean descendant = false;

	private Boolean doReduce = false;

	private Boolean inclusiveEnd = true;

	private Boolean includeDocs = true;

	private BurgerCbViewQueryStale stale = BurgerCbViewQueryStale.STALE_FALSE;

	// key와 {startKey, endKey}는 둘 중에 하나만 적용된다.
	private List<Object> key = new ArrayList<>();

	private List<Object> startKey = new ArrayList<>();
	private List<Object> endKey = new ArrayList<>();

	private String startKeyDocId = "";
	private String endKeyDocId = "";

	private Boolean group = false;
	private Integer groupLevel = 0;

	public BurgerCbViewQuery(String docName, String viewName) {
		this.docName = docName;
		this.viewName = viewName;
	}

	public String getDocName() {
		return docName;
	}

	public String getViewName() {
		return viewName;
	}

	public Integer getPageLimit() {
		return pageLimit;
	}

	public BurgerCbViewQuery setPageLimit(Integer pageLimit) {
		this.pageLimit = pageLimit;
		return this;
	}

	public Integer getPageNumber() {
		return pageNumber;
	}

	public BurgerCbViewQuery setPageNumber(Integer pageNumber) {
		this.pageNumber = pageNumber;
		return this;
	}

	public Boolean getDebugView() {
		return debugView;
	}

	public BurgerCbViewQuery setDebugView(Boolean debugView) {
		this.debugView = debugView;
		return this;
	}

	public Boolean getDescendant() {
		return descendant;
	}

	public BurgerCbViewQuery setDescendant(Boolean desc) {
		this.descendant = desc;
		return this;
	}

	public Boolean getDoReduce() {
		return doReduce;
	}

	public BurgerCbViewQuery setDoReduce(Boolean doReduce) {
		this.doReduce = doReduce;
		return this;
	}

	public List<Object> getKey() {
		return key;
	}

	public Boolean hasKey(Object o) {
		return key.contains(o);
	}

	public BurgerCbViewQuery setKey(Object key) {
		this.key.clear();
		this.key.add(key);

		this.startKey.clear();
		this.endKey.clear();

		return this;
	}

	// Keys 형태의 필터를 만들 때 사용해야 함. array key는 setArrayKey()를 사용
	public BurgerCbViewQuery addKey(Object key) {
		if (this.key.contains(key) == false) {
			this.key.add(key);
		}

		this.startKey.clear();
		this.endKey.clear();

		return this;
	}

	// Keys 형태의 필터를 만들 때 사용해야 함. array key는 setArrayKey()를 사용
	public BurgerCbViewQuery setKeys(List<Object> keys) {
		this.key = keys;

		this.startKey.clear();
		this.endKey.clear();

		return this;
	}

	public BurgerCbViewQuery setArrayKey(List<Object> arrayKey) {
		arrayKey.forEach(key -> {
			this.addArrayKey(key);
		});

		return this;
	}

	public BurgerCbViewQuery addArrayKey(Object key) {
		this.addStartKey(key);
		this.addEndKey(key);

		return this;
	}

	public BurgerCbViewQuery setStartKey(Object key) {
		this.startKey.clear();
		this.startKey.add(key);

		this.key.clear();

		return this;
	}

	public BurgerCbViewQuery addStartKey(Object key) {
		this.startKey.add(key);

		this.key.clear();

		return this;
	}

	public List<Object> getStartKey() {
		return this.startKey;
	}

	public BurgerCbViewQuery setEndKey(Object key) {
		this.endKey.clear();
		this.endKey.add(key);

		this.key.clear();

		return this;
	}

	public BurgerCbViewQuery addEndKey(Object key) {
		this.endKey.add(key);

		this.key.clear();

		return this;
	}

	public List<Object> getEndKey() {
		return this.endKey;
	}

	public BurgerCbViewQuery setStartKeyDocId(String id) {
		this.startKeyDocId = id;
		this.key.clear();

		return this;
	}

	public String getStartKeyDocId() {
		return this.startKeyDocId;
	}

	public BurgerCbViewQuery setEndKeyDocId(String id) {
		this.endKeyDocId = id;
		this.key.clear();

		return this;
	}

	public String getEndKeyDocId() {
		return this.endKeyDocId;
	}

	public Boolean getInclusiveEnd() {
		return inclusiveEnd;
	}

	public BurgerCbViewQuery setInclusiveEnd(Boolean inclusiveEnd) {
		this.inclusiveEnd = inclusiveEnd;

		return this;
	}

	public Boolean getIncludeDocs() {
		return includeDocs;
	}

	public BurgerCbViewQuery setIncludeDocs(boolean includeDocs) {
		this.includeDocs = includeDocs;

		return this;
	}

	public BurgerCbViewQueryStale getStale() {
		return this.stale;
	}

	public BurgerCbViewQuery setStale(BurgerCbViewQueryStale stale) {
		this.stale = stale;

		return this;
	}

	public BurgerCbViewQuery setStale(Boolean stale) {
		this.stale = BurgerCbViewQueryStale.from(stale);

		return this;
	}

	public Boolean getGroup() {
		return group;
	}

	public BurgerCbViewQuery setGroup(Boolean group) {
		this.group = group;

		return this;
	}

	public Integer getGroupLevel() {
		return groupLevel;
	}

	public BurgerCbViewQuery setGroupLevel(Integer groupLevel) {
		this.groupLevel = groupLevel;

		return this;
	}

	@Override
	public String toString() {
		String str = "TmonCbViewQuery : [" +
		        "docName = " + this.docName + ", " +
		        "viewName = " + this.viewName + ", " +
		        "pageLimit = " + this.pageLimit + ", " +
		        "pageNumber = " + this.pageNumber + ", " +
		        "debugView = " + this.debugView + ", " +
		        "key = " + this.key.toString() + ", " +
		        "startKey = " + this.startKey + ", " +
		        "endKey = " + this.endKey + ", " +
		        "startKeyDocId = " + this.startKeyDocId + ", " +
		        "inclusiveEnd = " + this.inclusiveEnd + ", " +
		        "doReduce = " + this.doReduce.toString() + ", " +
		        "group = " + this.group.toString() + ", " +
		        "groupLevel = " + this.groupLevel + ", " +
		        "stale = " + this.stale.toString();

		return str;
	}
}
