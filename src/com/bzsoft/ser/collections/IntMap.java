package com.bzsoft.ser.collections;

import java.util.Iterator;

/**
 * Same as 'java.util.Map' but uses primitive 'long' keys to minimise boxing
 * (and GC) overhead.
 * 
 */
public abstract class IntMap<V> {

	/** Iterates over LongMap key and values without boxing long keys */
	public static interface IntMapIterator<V> {

		boolean moveToNext();

		int key();

		V value();

		void remove();
	}

	/**
	 * Removes all mappings from this hash map, leaving it empty.
	 * 
	 * @see #isEmpty
	 * @see #size
	 */
	public abstract void clear();

	/**
	 * Returns the value of the mapping with the specified key.
	 * 
	 * @param key
	 *           the key.
	 * @return the value of the mapping with the specified key, or {@code null}
	 *         if no mapping for the specified key is found.
	 */
	public abstract V get(int key);

	/**
	 * Returns whether this map is empty.
	 * 
	 * @return {@code true} if this map has no elements, {@code false} otherwise.
	 * @see #size()
	 */
	public abstract boolean isEmpty();

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
	public abstract V put(int key, V value);

	/**
	 * Removes the mapping from this map
	 * 
	 * @param key
	 *           to remove
	 * @return value contained under this key, or null if value did not exist
	 */
	public abstract V remove(int key);

	/**
	 * Returns the number of elements in this map.
	 * 
	 * @return the number of elements in this map.
	 */
	public abstract int size();

	/**
	 * @return iterator over values in map
	 */
	public abstract Iterator<V> valuesIterator();

	public abstract IntMapIterator<V> intMapIterator();

	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder();
		b.append(getClass().getSimpleName());
		b.append('[');
		boolean first = true;
		final IntMapIterator<V> iter = intMapIterator();
		while (iter.moveToNext()) {
			if (first) {
				first = false;
			} else {
				b.append(", ");
			}
			b.append(iter.key());
			b.append(" => ");
			b.append(iter.value());
		}
		b.append(']');
		return b.toString();
	}
}