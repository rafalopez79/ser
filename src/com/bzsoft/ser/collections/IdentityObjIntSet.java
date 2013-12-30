package com.bzsoft.ser.collections;

import java.util.Random;

import com.bzsoft.ser.Utils;

public class IdentityObjIntSet<V> {

	/*
	 * Actual count of entries
	 */
	protected transient int				elementCount;

	/*
	 * The internal data structure to hold Entries
	 */
	protected transient Entry<V>[]	elementData;

	/*
	 * default size that an HashMap created using the default constructor would
	 * have.
	 */
	private static final int			DEFAULT_SIZE	= 16;

	/*
	 * maximum ratio of (stored elements)/(storage size) which does not lead to
	 * rehash
	 */
	protected final float				loadFactor;

	/**
	 * Salt added to keys before hashing, so it is harder to trigger hash
	 * collision attack.
	 */
	protected final long					hashSalt			= hashSaltValue();

	protected long hashSaltValue() {
		return new Random().nextLong();
	}

	/*
	 * maximum number of elements that can be put in this map before having to
	 * rehash
	 */
	protected int	threshold;

	private static class Entry<V> {
		final int	origKeyHash;

		final V		key;
		int			value;
		Entry<V>		next;

		public Entry(final V key, final int hash) {
			this.key = key;
			this.origKeyHash = hash;
		}
	}

	/**
	 * Create a new element array
	 * 
	 * @param s
	 * @return Reference to the element array
	 */
	@SuppressWarnings("unchecked")
	protected Entry<V>[] newElementArray(final int s) {
		return new Entry[s];
	}

	/**
	 * Constructs a new empty {@code HashMap} instance.
	 */
	public IdentityObjIntSet() {
		this(DEFAULT_SIZE);
	}

	/**
	 * Constructs a new {@code HashMap} instance with the specified capacity.
	 * 
	 * @param capacity
	 *           the initial capacity of this hash map.
	 * @throws IllegalArgumentException
	 *            when the capacity is less than zero.
	 */
	public IdentityObjIntSet(final int capacity) {
		this(capacity, 0.75f); // default load factor of 0.75
	}

	/**
	 * Calculates the capacity of storage required for storing given number of
	 * elements
	 * 
	 * @param x
	 *           number of elements
	 * @return storage size
	 */
	private static final int calculateCapacity(int x) {
		if (x >= 1 << 30) {
			return 1 << 30;
		}
		if (x == 0) {
			return 16;
		}
		x = x - 1;
		x |= x >> 1;
		x |= x >> 2;
		x |= x >> 4;
		x |= x >> 8;
		x |= x >> 16;
		return x + 1;
	}

	/**
	 * Constructs a new {@code HashMap} instance with the specified capacity and
	 * load factor.
	 * 
	 * @param capacity
	 *           the initial capacity of this hash map.
	 * @param loadFactor
	 *           the initial load factor.
	 * @throws IllegalArgumentException
	 *            when the capacity is less than zero or the load factor is less
	 *            or equal to zero.
	 */
	public IdentityObjIntSet(int capacity, final float loadFactor) {
		if (capacity >= 0 && loadFactor > 0) {
			capacity = calculateCapacity(capacity);
			elementCount = 0;
			elementData = newElementArray(capacity);
			this.loadFactor = loadFactor;
			computeThreshold();
		} else {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Removes all mappings from this hash map, leaving it empty.
	 * 
	 * @see #isEmpty
	 * @see #size
	 */
	public void clear() {
		if (elementCount > 0) {
			for (int i = 0; i < elementCount; i++) {
				elementData[i] = null;
			}
			elementCount = 0;
		}
	}

	/**
	 * Computes the threshold for rehashing
	 */
	private void computeThreshold() {
		threshold = (int) (elementData.length * loadFactor);
	}

	public int indexOf(final V key) {
		final Entry<V> m = getEntry(key);
		if (m != null) {
			return m.value;
		}
		return -1;
	}

	protected final Entry<V> getEntry(final V v) {
		final int key = System.identityHashCode(v);
		final int hash = Utils.longHash(key ^ hashSalt);
		final int index = hash & elementData.length - 1;
		return findNonNullKeyEntry(v, index, hash);
	}

	protected final Entry<V> findNonNullKeyEntry(final V key, final int index, final int keyHash) {
		Entry<V> m = elementData[index];
		while (m != null && (m.origKeyHash != keyHash || key != m.key)) {
			m = m.next;
		}
		return m;
	}

	public boolean isEmpty() {
		return elementCount == 0;
	}

	public void add(final V v, final int value) {
		Entry<V> entry;
		final int key = System.identityHashCode(v);
		final int hash = Utils.longHash(key ^ hashSalt);
		final int index = hash & elementData.length - 1;
		entry = findNonNullKeyEntry(v, index, hash);
		if (entry == null) {
			entry = createHashedEntry(v, index, hash);
			if (++elementCount > threshold) {
				rehash();
			}
		}
		entry.value = value;
	}

	protected Entry<V> createHashedEntry(final V key, final int index, final int hash) {
		final Entry<V> entry = new Entry<V>(key, hash);
		entry.next = elementData[index];
		elementData[index] = entry;
		return entry;
	}

	protected void rehash(final int capacity) {
		final int length = calculateCapacity(capacity == 0 ? 1 : capacity << 1);
		final Entry<V>[] newData = newElementArray(length);
		for (int i = 0; i < elementData.length; i++) {
			Entry<V> entry = elementData[i];
			elementData[i] = null;
			while (entry != null) {
				final int index = entry.origKeyHash & length - 1;
				final Entry<V> next = entry.next;
				entry.next = newData[index];
				newData[index] = entry;
				entry = next;
			}
		}
		elementData = newData;
		computeThreshold();
	}

	protected void rehash() {
		rehash(elementData.length);
	}

	public void remove(final V value) {
		int index = 0;
		Entry<V> entry;
		Entry<V> last = null;
		final int key = System.identityHashCode(value);
		final int hash = Utils.longHash(key ^ hashSalt);
		index = hash & elementData.length - 1;
		entry = elementData[index];
		while (entry != null && !(entry.origKeyHash == hash && value == entry.key)) {
			last = entry;
			entry = entry.next;
		}
		if (entry == null) {
			return;
		}
		if (last == null) {
			elementData[index] = entry.next;
		} else {
			last.next = entry.next;
		}
		elementCount--;
	}

	public int size() {
		return elementCount;
	}

}