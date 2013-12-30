package com.bzsoft.ser.collections;

import java.util.Arrays;
import java.util.Random;

import com.bzsoft.ser.Utils;

/**
 * IntHashMap is an implementation of LongMap without concurrency locking. This
 * code is adoption of 'HashMap' from Apache Harmony refactored to support
 * primitive in keys.
 * 
 * Get returns -1 if not found
 */
public class IntIntHashMap extends IntIntMap {

	/*
	 * Actual count of entries
	 */
	protected transient int			elementCount;

	/*
	 * The internal data structure to hold Entries
	 */
	protected transient Entry[]	elementData;

	/*
	 * default size that an HashMap created using the default constructor would
	 * have.
	 */
	private static final int		DEFAULT_SIZE	= 16;

	/*
	 * maximum ratio of (stored elements)/(storage size) which does not lead to
	 * rehash
	 */
	protected final float			loadFactor;

	/**
	 * Salt added to keys before hashing, so it is harder to trigger hash
	 * collision attack.
	 */
	protected final long				hashSalt			= hashSaltValue();

	protected long hashSaltValue() {
		return new Random().nextLong();
	}

	/*
	 * maximum number of elements that can be put in this map before having to
	 * rehash
	 */
	protected int	threshold;

	private static final class Entry {
		final int	origKeyHash;
		final int	key;
		int			value;
		Entry			next;

		public Entry(final int key, final int hash) {
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
	protected Entry[] newElementArray(final int s) {
		return new Entry[s];
	}

	/**
	 * Constructs a new empty {@code HashMap} instance.
	 */
	public IntIntHashMap() {
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
	public IntIntHashMap(final int capacity) {
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
	public IntIntHashMap(int capacity, final float loadFactor) {
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
	@Override
	public void clear() {
		if (elementCount > 0) {
			elementCount = 0;
			Arrays.fill(elementData, null);
		}
	}

	/**
	 * Computes the threshold for rehashing
	 */
	private void computeThreshold() {
		threshold = (int) (elementData.length * loadFactor);
	}

	/**
	 * Returns the value of the mapping with the specified key.
	 * 
	 * @param key
	 *           the key.
	 * @return the value of the mapping with the specified key, or {@code null}
	 *         if no mapping for the specified key is found.
	 */
	@Override
	public int get(final int key) {
		final Entry m = getEntry(key);
		if (m != null) {
			return m.value;
		}
		return -1;
	}

	protected final Entry getEntry(final int key) {
		final int hash = Utils.longHash(key ^ hashSalt);
		final int index = hash & elementData.length - 1;
		return findNonNullKeyEntry(key, index, hash);
	}

	protected final Entry findNonNullKeyEntry(final long key, final int index, final int keyHash) {
		Entry m = elementData[index];
		while (m != null && (m.origKeyHash != keyHash || key != m.key)) {
			m = m.next;
		}
		return m;
	}

	/**
	 * Returns whether this map is empty.
	 * 
	 * @return {@code true} if this map has no elements, {@code false} otherwise.
	 * @see #size()
	 */
	@Override
	public boolean isEmpty() {
		return elementCount == 0;
	}

	/**
	 * Maps the specified key to the specified value.
	 * 
	 * @param key
	 *           the key.
	 * @param value
	 *           the value.
	 * @return the value of any previous mapping with the specified key or
	 *         {@code null} if there was no such mapping.
	 */
	@Override
	public int put(final int key, final int value) {
		Entry entry;
		final int hash = Utils.longHash(key ^ hashSalt);
		final int index = hash & elementData.length - 1;
		entry = findNonNullKeyEntry(key, index, hash);
		if (entry == null) {
			entry = createHashedEntry(key, index, hash);
			if (++elementCount > threshold) {
				rehash();
			}
		}

		final int result = entry.value;
		entry.value = value;
		return result;
	}

	protected Entry createHashedEntry(final int key, final int index, final int hash) {
		final Entry entry = new Entry(key, hash);
		entry.next = elementData[index];
		elementData[index] = entry;
		return entry;
	}

	protected void rehash(final int capacity) {
		final int length = calculateCapacity(capacity == 0 ? 1 : capacity << 1);
		final Entry[] newData = newElementArray(length);
		for (int i = 0; i < elementData.length; i++) {
			Entry entry = elementData[i];
			elementData[i] = null;
			while (entry != null) {
				final int index = entry.origKeyHash & length - 1;
				final Entry next = entry.next;
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

	/**
	 * Removes the mapping with the specified key from this map.
	 * 
	 * @param key
	 *           the key of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping for
	 *         the specified key was found.
	 */
	@Override
	public void remove(final int key) {
		removeEntry(key);
	}

	protected final Entry removeEntry(final int key) {
		int index = 0;
		Entry entry;
		Entry last = null;
		final int hash = Utils.longHash(key ^ hashSalt);
		index = hash & elementData.length - 1;
		entry = elementData[index];
		while (entry != null && !(entry.origKeyHash == hash && key == entry.key)) {
			last = entry;
			entry = entry.next;
		}
		if (entry == null) {
			return null;
		}
		if (last == null) {
			elementData[index] = entry.next;
		} else {
			last.next = entry.next;
		}
		elementCount--;
		return entry;
	}

	/**
	 * Returns the number of elements in this map.
	 * 
	 * @return the number of elements in this map.
	 */
	@Override
	public int size() {
		return elementCount;
	}
}