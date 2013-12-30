package com.bzsoft.ser;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;

final class ReferenceSet<K> {

	private final Map<K, Integer>	set;
	private K[]							data;
	private int							size;

	@SuppressWarnings("unchecked")
	protected ReferenceSet() {
		set = new IdentityHashMap<K, Integer>();
		data = (K[]) new Object[32];
		size = 0;
	}

	protected void add(final K o) {
		set.put(o, size);
		if (data.length == size) {
			data = Arrays.copyOf(data, data.length * 2);
		}
		data[size] = o;
		size++;
	}

	protected int indexOf(final K obj) {
		final Integer index = set.get(obj);
		if (index == null) {
			return -1;
		}
		return index;
	}

	protected K elementOf(final int index) {
		final K ret = data[index];
		return ret;
	}

	protected int getSize() {
		return size;
	}

	protected void clear() {
		set.clear();
		for (int i = 0; i < size; i++) {
			data[i] = null;
		}
		size = 0;
	}

	@Override
	public String toString() {
		return "ReferenceSet [size=" + set.size() + ", " + Arrays.toString(data) + "]";
	}

}
