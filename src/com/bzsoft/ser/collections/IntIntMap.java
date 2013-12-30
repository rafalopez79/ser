package com.bzsoft.ser.collections;

/**
 * Same as 'java.util.Map' but uses primitive 'long' keys to minimise boxing
 * (and GC) overhead.
 * 
 */
public abstract class IntIntMap {

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
	public abstract int get(int key);

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
	public abstract int put(int key, int value);

	/**
	 * Removes the mapping from this map
	 * 
	 * @param key
	 *           to remove
	 * @return value contained under this key, or null if value did not exist
	 */
	public abstract void remove(int key);

	/**
	 * Returns the number of elements in this map.
	 * 
	 * @return the number of elements in this map.
	 */
	public abstract int size();

	@Override
	public String toString() {
		final StringBuilder b = new StringBuilder();
		b.append(getClass().getSimpleName());
		b.append('[');
		b.append(size());
		b.append(']');
		return b.toString();
	}
}