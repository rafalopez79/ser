package com.bzsoft.ser.collections;

import java.io.Serializable;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Random;

import com.bzsoft.ser.Utils;

/**
 * LongHashMap is an implementation of LongMap without concurrency locking. This
 * code is adoption of 'HashMap' from Apache Harmony refactored to support
 * primitive long keys.
 */
public class LongHashMap<V> extends LongMap<V> implements Serializable {

	private static final long			serialVersionUID	= 362340234235222265L;

	/*
	 * Actual count of entries
	 */
	protected transient int				elementCount;

	/*
	 * The internal data structure to hold Entries
	 */
	protected transient Entry<V>[]	elementData;

	/*
	 * modification count, to keep track of structural modifications between the
	 * HashMap and the iterator
	 */
	protected transient int				modCount				= 0;

	/*
	 * default size that an HashMap created using the default constructor would
	 * have.
	 */
	private static final int			DEFAULT_SIZE		= 16;

	/*
	 * maximum ratio of (stored elements)/(storage size) which does not lead to
	 * rehash
	 */
	protected final float				loadFactor;

	/**
	 * Salt added to keys before hashing, so it is harder to trigger hash
	 * collision attack.
	 */
	protected final long					hashSalt				= hashSaltValue();

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

		final long	key;
		V				value;
		Entry<V>		next;

		public Entry(final long key, final int hash) {
			this.key = key;
			this.origKeyHash = hash;
		}
	}

	private static class AbstractMapIterator<V> {
		protected int							position	= 0;
		protected int							expectedModCount;
		protected Entry<V>					futureEntry;
		protected Entry<V>					currentEntry;
		protected Entry<V>					prevEntry;

		protected final LongHashMap<V>	associatedMap;

		protected AbstractMapIterator(final LongHashMap<V> hm) {
			associatedMap = hm;
			expectedModCount = hm.modCount;
			futureEntry = null;
		}

		public boolean hasNext() {
			if (futureEntry != null) {
				return true;
			}
			while (position < associatedMap.elementData.length) {
				if (associatedMap.elementData[position] == null) {
					position++;
				} else {
					return true;
				}
			}
			return false;
		}

		final void checkConcurrentMod() throws ConcurrentModificationException {
			if (expectedModCount != associatedMap.modCount) {
				throw new ConcurrentModificationException();
			}
		}

		final void makeNext() {
			checkConcurrentMod();
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			if (futureEntry == null) {
				currentEntry = associatedMap.elementData[position++];
				futureEntry = currentEntry.next;
				prevEntry = null;
			} else {
				if (currentEntry != null) {
					prevEntry = currentEntry;
				}
				currentEntry = futureEntry;
				futureEntry = futureEntry.next;
			}
		}

		public final void remove() {
			checkConcurrentMod();
			if (currentEntry == null) {
				throw new IllegalStateException();
			}
			if (prevEntry == null) {
				final int index = currentEntry.origKeyHash & associatedMap.elementData.length - 1;
				associatedMap.elementData[index] = associatedMap.elementData[index].next;
			} else {
				prevEntry.next = currentEntry.next;
			}
			currentEntry = null;
			expectedModCount++;
			associatedMap.modCount++;
			associatedMap.elementCount--;

		}
	}

	private static class EntryIterator<V> extends AbstractMapIterator<V> implements LongMapIterator<V> {

		protected EntryIterator(final LongHashMap<V> map) {
			super(map);
		}

		@Override
		public boolean moveToNext() {
			if (!hasNext()) {
				return false;
			}
			makeNext();
			return true;
		}

		@Override
		public long key() {
			return currentEntry.key;
		}

		@Override
		public V value() {
			return currentEntry.value;
		}
	}

	private static class ValueIterator<V> extends AbstractMapIterator<V> implements Iterator<V> {

		protected ValueIterator(final LongHashMap<V> map) {
			super(map);
		}

		@Override
		public V next() {
			makeNext();
			return currentEntry.value;
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
	public LongHashMap() {
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
	public LongHashMap(final int capacity) {
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
	public LongHashMap(int capacity, final float loadFactor) {
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
			modCount++;
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
	public V get(final long key) {
		final Entry<V> m = getEntry(key);
		if (m != null) {
			return m.value;
		}
		return null;
	}

	protected final Entry<V> getEntry(final long key) {
		final int hash = Utils.longHash(key ^ hashSalt);
		final int index = hash & elementData.length - 1;
		return findNonNullKeyEntry(key, index, hash);
	}

	protected final Entry<V> findNonNullKeyEntry(final long key, final int index, final int keyHash) {
		Entry<V> m = elementData[index];
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
	public V put(final long key, final V value) {
		Entry<V> entry;
		final int hash = Utils.longHash(key ^ hashSalt);
		final int index = hash & elementData.length - 1;
		entry = findNonNullKeyEntry(key, index, hash);
		if (entry == null) {
			modCount++;
			entry = createHashedEntry(key, index, hash);
			if (++elementCount > threshold) {
				rehash();
			}
		}

		final V result = entry.value;
		entry.value = value;
		return result;
	}

	protected Entry<V> createHashedEntry(final long key, final int index, final int hash) {
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

	/**
	 * Removes the mapping with the specified key from this map.
	 * 
	 * @param key
	 *           the key of the mapping to remove.
	 * @return the value of the removed mapping or {@code null} if no mapping for
	 *         the specified key was found.
	 */
	@Override
	public V remove(final long key) {
		final Entry<V> entry = removeEntry(key);
		if (entry != null) {
			return entry.value;
		}
		return null;
	}

	protected final Entry<V> removeEntry(final long key) {
		int index = 0;
		Entry<V> entry;
		Entry<V> last = null;

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
		modCount++;
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

	@Override
	public Iterator<V> valuesIterator() {
		return new ValueIterator<V>(this);
	}

	@Override
	public LongMapIterator<V> longMapIterator() {
		return new EntryIterator<V>(this);
	}

}