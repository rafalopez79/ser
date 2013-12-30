package com.bzsoft.ser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

public final class Utils {

	private static final char[]	HEX_CHARS		= { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };

	public static final String		EMPTY_STRING	= "";
	public static final String		UTF8				= "UTF8";
	public static final Charset	UTF8_CHARSET	= Charset.forName(UTF8);

	private Utils() {
		// empty
	}

	/**
	 * Pack non-negative long into output stream. It will occupy 1-10 bytes
	 * depending on value (lower values occupy smaller space)
	 * 
	 * @param out
	 *           DataOutput to put value into
	 * @param value
	 *           to be serialized, must be non-negative
	 * @throws IOException
	 *            Signals that an I/O exception has occurred.
	 */
	public static void packLong(final DataOutput out, long value) throws IOException {
		assert value >= 0 : "negative value: " + value;
		while ((value & ~0x7FL) != 0) {
			out.write((int) value & 0x7F | 0x80);
			value >>>= 7;
		}
		out.writeByte((byte) value);
	}

	/**
	 * Unpack positive long value from the input stream.
	 * 
	 * @param in
	 *           The input stream.
	 * @return The long value.
	 * @throws IOException
	 *            Signals that an I/O exception has occurred.
	 */
	public static long unpackLong(final DataInput in) throws IOException {
		long result = 0;
		for (int offset = 0; offset < 64; offset += 7) {
			final long b = in.readUnsignedByte();
			result |= (b & 0x7F) << offset;
			if ((b & 0x80) == 0) {
				return result;
			}
		}
		throw new Error("Malformed long.");
	}

	/**
	 * get number of bytes occupied by packed long.
	 * 
	 * @param value
	 *           the value
	 * @return the int
	 */
	public static int packedLongSize(long value) {
		int ret = 1;
		while ((value & ~0x7FL) != 0) {
			ret++;
			value >>>= 7;
		}
		return ret;
	}

	/**
	 * Pack non-negative long into output stream. It will occupy 1-5 bytes
	 * depending on value (lower values occupy smaller space)
	 * 
	 * @param in
	 *           DataOutput to put value into
	 * @param value
	 *           to be serialized, must be non-negative
	 * @throws IOException
	 *            Signals that an I/O exception has occurred.
	 */

	public static void packInt(final DataOutput in, int value) throws IOException {
		assert value >= 0 : "negative value: " + value;
		while ((value & ~0x7F) != 0) {
			in.write(value & 0x7F | 0x80);
			value >>>= 7;
		}
		in.writeByte((byte) value);
	}

	/**
	 * Unpack int.
	 * 
	 * @param is
	 *           the is
	 * @return the int
	 * @throws IOException
	 *            Signals that an I/O exception has occurred.
	 */
	public static int unpackInt(final DataInput is) throws IOException {
		for (int offset = 0, result = 0; offset < 32; offset += 7) {
			final int b = is.readUnsignedByte();
			result |= (b & 0x7F) << offset;
			if ((b & 0x80) == 0) {
				return result;
			}
		}
		throw new Error("Malformed int.");
	}

	/**
	 * Long hash.
	 * 
	 * @param key
	 *           the key
	 * @return the int
	 */
	public static int longHash(final long key) {
		int h = (int) (key ^ key >>> 32);
		h ^= h >>> 20 ^ h >>> 12;
		return h ^ h >>> 7 ^ h >>> 4;
	}

	/**
	 * Int hash.
	 * 
	 * @param h
	 *           the h
	 * @return the int
	 */
	public static int intHash(int h) {
		h ^= h >>> 20 ^ h >>> 12;
		return h ^ h >>> 7 ^ h >>> 4;
	}

	/**
	 * expand array size by 1, and put value at given position. No items from
	 * original array are lost
	 * 
	 * @param array
	 *           the array
	 * @param pos
	 *           the pos
	 * @param value
	 *           the value
	 * @return the object[]
	 */
	public static Object[] arrayPut(final Object[] array, final int pos, final Object value) {
		final Object[] ret = Arrays.copyOf(array, array.length + 1);
		if (pos < array.length) {
			System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
		}
		ret[pos] = value;
		return ret;
	}

	/**
	 * Array long put.
	 * 
	 * @param array
	 *           the array
	 * @param pos
	 *           the pos
	 * @param value
	 *           the value
	 * @return the long[]
	 */
	public static long[] arrayLongPut(final long[] array, final int pos, final long value) {
		final long[] ret = Arrays.copyOf(array, array.length + 1);
		if (pos < array.length) {
			System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
		}
		ret[pos] = value;
		return ret;
	}

	/**
	 * Compute nearest bigger power of two.
	 * 
	 * @param value
	 *           the value
	 * @return the int
	 */
	public static int nextPowTwo(final int value) {
		int ret = 2;
		while (ret < value) {
			ret = ret << 1;
		}
		return ret;
	}

	/**
	 * Random string.
	 * 
	 * @param size
	 *           the size
	 * @return the string
	 */
	public static String randomString(final int size) {
		final String chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\";
		final StringBuilder b = new StringBuilder(size);
		final Random r = new Random();
		for (int i = 0; i < size; i++) {
			b.append(chars.charAt(r.nextInt(chars.length())));
		}
		return b.toString();
	}

	/**
	 * To hexa.
	 * 
	 * @param bb
	 *           the bb
	 * @return the string
	 */
	public static String toHex(final byte[] bb) {
		final char[] ret = new char[bb.length * 2];
		for (int i = 0; i < bb.length; i++) {
			ret[i * 2] = HEX_CHARS[(bb[i] & 0xF0) >> 4];
			ret[i * 2 + 1] = HEX_CHARS[bb[i] & 0x0F];
		}
		return new String(ret);
	}

	/**
	 * From hexa.
	 * 
	 * @param s
	 *           the s
	 * @return the byte[]
	 */
	public static byte[] fromHex(final String s) {
		final byte[] ret = new byte[s.length() / 2];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
		}
		return ret;
	}
}
