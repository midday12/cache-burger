package com.midday.burger.couchbaseburger.view;

import com.couchbase.client.java.view.Stale;

/**
 * Created by midday on 2015-08-18.
 * Stale 값 지정
 */
public enum BurgerCbViewQueryStale {
	/* */STALE_FALSE(0, Stale.FALSE),
	/* */STALE_TRUE(1, Stale.TRUE),
	/* */STALE_UPDATE_AFTER(-1, Stale.UPDATE_AFTER);

	private int value = 0;
	private Stale stale = Stale.FALSE;

	BurgerCbViewQueryStale(int value, Stale stale) {
		this.value = value;
		this.stale = stale;
	}

	public Stale getStale() {
		return stale;
	}

	public void setStale(Stale stale) {
		this.stale = stale;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}

	public static BurgerCbViewQueryStale from(Boolean stale) {
		if (stale == true) {
			return STALE_UPDATE_AFTER;
		} else {
			return STALE_FALSE;
		}
	}
}
