package com.bzsoft.ser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.StreamCorruptedException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

public abstract class BaseSerializer implements Serializer<Object> {

	protected static final class Header {

		private Header() {
			// util class
		}

		public static final int	ZERO_FAIL						= 0;
		public static final int	NULL								= 1;
		public static final int	BOOLEAN_TRUE					= 2;
		public static final int	BOOLEAN_FALSE					= 3;

		public static final int	INT_M9							= 4;
		public static final int	INT_M8							= 5;
		public static final int	INT_M7							= 6;
		public static final int	INT_M6							= 7;
		public static final int	INT_M5							= 8;
		public static final int	INT_M4							= 9;
		public static final int	INT_M3							= 10;
		public static final int	INT_M2							= 11;
		public static final int	INT_M1							= 12;
		public static final int	INT_0								= 13;
		public static final int	INT_1								= 14;
		public static final int	INT_2								= 15;
		public static final int	INT_3								= 16;
		public static final int	INT_4								= 17;
		public static final int	INT_5								= 18;
		public static final int	INT_6								= 19;
		public static final int	INT_7								= 20;
		public static final int	INT_8								= 21;
		public static final int	INT_9								= 22;
		public static final int	INT_10							= 23;
		public static final int	INT_11							= 24;
		public static final int	INT_12							= 25;
		public static final int	INT_13							= 26;
		public static final int	INT_14							= 27;
		public static final int	INT_15							= 28;
		public static final int	INT_16							= 29;
		public static final int	INT_MIN_VALUE					= 30;
		public static final int	INT_MAX_VALUE					= 31;
		public static final int	INT_MF1							= 32;
		public static final int	INT_F1							= 33;
		public static final int	INT_MF2							= 34;
		public static final int	INT_F2							= 35;
		public static final int	INT_MF3							= 36;
		public static final int	INT_F3							= 37;
		public static final int	INT								= 38;

		public static final int	LONG_M9							= 39;
		public static final int	LONG_M8							= 40;
		public static final int	LONG_M7							= 41;
		public static final int	LONG_M6							= 42;
		public static final int	LONG_M5							= 43;
		public static final int	LONG_M4							= 44;
		public static final int	LONG_M3							= 45;
		public static final int	LONG_M2							= 46;
		public static final int	LONG_M1							= 47;
		public static final int	LONG_0							= 48;
		public static final int	LONG_1							= 49;
		public static final int	LONG_2							= 50;
		public static final int	LONG_3							= 51;
		public static final int	LONG_4							= 52;
		public static final int	LONG_5							= 53;
		public static final int	LONG_6							= 54;
		public static final int	LONG_7							= 55;
		public static final int	LONG_8							= 56;
		public static final int	LONG_9							= 57;
		public static final int	LONG_10							= 58;
		public static final int	LONG_11							= 59;
		public static final int	LONG_12							= 60;
		public static final int	LONG_13							= 61;
		public static final int	LONG_14							= 62;
		public static final int	LONG_15							= 63;
		public static final int	LONG_16							= 64;
		public static final int	LONG_MIN_VALUE					= 65;
		public static final int	LONG_MAX_VALUE					= 66;
		public static final int	LONG_MF1							= 67;
		public static final int	LONG_F1							= 68;
		public static final int	LONG_MF2							= 69;
		public static final int	LONG_F2							= 70;
		public static final int	LONG_MF3							= 71;
		public static final int	LONG_F3							= 72;
		public static final int	LONG_MF4							= 73;
		public static final int	LONG_F4							= 74;
		public static final int	LONG_MF5							= 75;
		public static final int	LONG_F5							= 76;
		public static final int	LONG_MF6							= 77;
		public static final int	LONG_F6							= 78;
		public static final int	LONG_MF7							= 79;
		public static final int	LONG_F7							= 80;
		public static final int	LONG								= 81;

		public static final int	BYTE_M1							= 82;
		public static final int	BYTE_0							= 83;
		public static final int	BYTE_1							= 84;
		public static final int	BYTE								= 85;
		public static final int	CHAR_0							= 86;
		public static final int	CHAR_1							= 87;
		public static final int	CHAR_255							= 88;
		public static final int	CHAR								= 89;
		public static final int	SHORT_M1							= 90;
		public static final int	SHORT_0							= 91;
		public static final int	SHORT_1							= 92;
		public static final int	SHORT_255						= 93;
		public static final int	SHORT_M255						= 94;
		public static final int	SHORT								= 95;
		public static final int	FLOAT_M1							= 96;
		public static final int	FLOAT_0							= 97;
		public static final int	FLOAT_1							= 98;
		public static final int	FLOAT_255						= 99;
		public static final int	FLOAT_SHORT						= 100;
		public static final int	FLOAT								= 101;
		public static final int	DOUBLE_M1						= 102;
		public static final int	DOUBLE_0							= 103;
		public static final int	DOUBLE_1							= 104;
		public static final int	DOUBLE_255						= 105;
		public static final int	DOUBLE_SHORT					= 106;
		public static final int	DOUBLE_INT						= 107;
		public static final int	DOUBLE							= 108;
		public static final int	ARRAY_BYTE						= 109;
		public static final int	ARRAY_BYTE_ALL_EQUAL			= 110;
		public static final int	ARRAY_BOOLEAN					= 111;
		public static final int	ARRAY_SHORT						= 112;
		public static final int	ARRAY_CHAR						= 113;
		public static final int	ARRAY_FLOAT						= 114;
		public static final int	ARRAY_DOUBLE					= 115;
		public static final int	ARRAY_INT_BYTE					= 116;
		public static final int	ARRAY_INT_SHORT				= 117;
		public static final int	ARRAY_INT_PACKED				= 118;
		public static final int	ARRAY_INT						= 119;
		public static final int	ARRAY_LONG_BYTE				= 120;
		public static final int	ARRAY_LONG_SHORT				= 121;
		public static final int	ARRAY_LONG_PACKED				= 122;
		public static final int	ARRAY_LONG_INT					= 123;
		public static final int	ARRAY_LONG						= 124;
		public static final int	STRING_0							= 125;
		public static final int	STRING_1							= 126;
		public static final int	STRING_2							= 127;
		public static final int	STRING_3							= 128;
		public static final int	STRING_4							= 129;
		public static final int	STRING_5							= 130;
		public static final int	STRING_6							= 131;
		public static final int	STRING_7							= 132;
		public static final int	STRING_8							= 133;
		public static final int	STRING_9							= 134;
		public static final int	STRING_10						= 135;
		public static final int	STRING							= 136;
		public static final int	BIGDECIMAL						= 137;
		public static final int	BIGINTEGER						= 138;
		public static final int	CLASS								= 139;
		public static final int	DATE								= 140;
		public static final int	UUID								= 142;

		// 144 to 149 reserved for other non recursive objects

		public static final int	ARRAY_OBJECT					= 158;
		// special cases for BTree values which stores references
		public static final int	ARRAY_OBJECT_PACKED_LONG	= 159;
		public static final int	ARRAYLIST_PACKED_LONG		= 160;
		public static final int	ARRAY_OBJECT_ALL_NULL		= 161;
		public static final int	ARRAY_OBJECT_NO_REFS			= 162;
		public static final int	ARRAYLIST						= 163;
		public static final int	TREEMAP							= 164;
		public static final int	HASHMAP							= 165;
		public static final int	LINKEDHASHMAP					= 166;
		public static final int	TREESET							= 167;
		public static final int	HASHSET							= 168;
		public static final int	LINKEDHASHSET					= 169;
		public static final int	LINKEDLIST						= 170;
		public static final int	PROPERTIES						= 171;
		public static final int	JAVA_SERIALIZATION			= 172;
		public static final int	BEAN								= 173;
		public static final int	OBJECT_STACK					= 174;

		public static final int	CLASS_ID							= 189;
		public static final int	CLASS_NID						= 190;

		public static final int	RESET								= 254;
	}

	/**
	 * Utility class similar to ArrayList, but with fast identity search.
	 */

	@Override
	public void serializeObject(final DataOutput out, final Object obj, final ReferenceSet<Object> rset) throws IOException {
		serialize(out, obj, rset);
	}

	protected void serialize(final DataOutput out, final Object obj, ReferenceSet<Object> objectStack) throws IOException {
		if (obj == null) {
			out.writeByte(Header.NULL);
			return;
		}
		/** try to find object on stack if it exists */
		if (objectStack != null) {
			final int index = objectStack.indexOf(obj);
			if (index != -1) {
				// object was already serialized, just write reference to it
				// and return
				out.writeByte(Header.OBJECT_STACK);
				Utils.packInt(out, index);
				return;
			}
			// add this object to objectStack
			objectStack.add(obj);
		}
		final Class<?> clazz = obj.getClass();
		if (clazz == Integer.class) {
			serializeInt(out, (Integer) obj);
			return;
		} else if (clazz == Long.class) {
			serializeLong(out, (Long) obj);
			return;
		} else if (clazz == String.class) {
			serializeString(out, (String) obj, null);
			return;
		} else if (clazz == Boolean.class) {
			out.writeByte((Boolean) obj ? Header.BOOLEAN_TRUE : Header.BOOLEAN_FALSE);
			return;
		} else if (clazz == Byte.class) {
			serializeByte(out, (Byte) obj);
			return;
		} else if (clazz == Character.class) {
			serializeChar(out, (Character) obj);
			return;
		} else if (clazz == Short.class) {
			serializeShort(out, (Short) obj);
			return;
		} else if (clazz == Float.class) {
			serializeFloat(out, (Float) obj);
			return;
		} else if (clazz == Double.class) {
			serializeDouble(out, (Double) obj);
			return;
		} else if (obj instanceof byte[]) {
			final byte[] b = (byte[]) obj;
			serializeByteArray(out, b);
			return;
		} else if (obj instanceof boolean[]) {
			out.writeByte(Header.ARRAY_BOOLEAN);
			final boolean[] abool = (boolean[]) obj;
			Utils.packInt(out, abool.length);// write the number of booleans
			final byte[] a = booleanToByteArray(abool);
			out.write(a);
			return;
		} else if (obj instanceof short[]) {
			out.writeByte(Header.ARRAY_SHORT);
			final short[] a = (short[]) obj;
			Utils.packInt(out, a.length);
			for (final short s : a) {
				out.writeShort(s);
			}
			return;
		} else if (obj instanceof char[]) {
			out.writeByte(Header.ARRAY_CHAR);
			final char[] a = (char[]) obj;
			Utils.packInt(out, a.length);
			for (final char s : a) {
				out.writeChar(s);
			}
			return;
		} else if (obj instanceof float[]) {
			out.writeByte(Header.ARRAY_FLOAT);
			final float[] a = (float[]) obj;
			Utils.packInt(out, a.length);
			for (final float s : a) {
				out.writeFloat(s);
			}
			return;
		} else if (obj instanceof double[]) {
			out.writeByte(Header.ARRAY_DOUBLE);
			final double[] a = (double[]) obj;
			Utils.packInt(out, a.length);
			for (final double s : a) {
				out.writeDouble(s);
			}
			return;
		} else if (obj instanceof int[]) {
			serializeIntArray(out, (int[]) obj);
			return;
		} else if (obj instanceof long[]) {
			serializeLongArray(out, (long[]) obj);
			return;
		} else if (clazz == BigInteger.class) {
			out.writeByte(Header.BIGINTEGER);
			final byte[] buf = ((BigInteger) obj).toByteArray();
			Utils.packInt(out, buf.length);
			out.write(buf);
			return;
		} else if (clazz == BigDecimal.class) {
			out.writeByte(Header.BIGDECIMAL);
			final BigDecimal d = (BigDecimal) obj;
			final byte[] buf = d.unscaledValue().toByteArray();
			Utils.packInt(out, buf.length);
			out.write(buf);
			Utils.packInt(out, d.scale());
			return;
		} else if (obj instanceof Class) {
			out.writeByte(Header.CLASS);
			serializeClass(out, (Class<?>) obj);
			return;
		} else if (clazz == Date.class) {
			out.writeByte(Header.DATE);
			out.writeLong(((Date) obj).getTime());
			return;
		} else if (clazz == UUID.class) {
			out.writeByte(Header.UUID);
			final UUID uuid = (UUID) obj;
			out.writeLong(uuid.getMostSignificantBits());
			out.writeLong(uuid.getLeastSignificantBits());
			return;
		}

		/**
		 * classes bellow need object stack, so initialize it if not alredy
		 * initialized
		 */
		if (objectStack == null) {
			objectStack = new ReferenceSet<Object>();
			objectStack.add(obj);
		}
		if (obj instanceof Object[]) {
			final Object[] b = (Object[]) obj;
			boolean packableLongs = b.length <= 255;
			boolean allNull = true;
			if (packableLongs) {
				// check if it contains packable longs
				for (final Object o : b) {
					if (o != null) {
						allNull = false;
						if (o.getClass() != Long.class || (Long) o < 0 && (Long) o != Long.MAX_VALUE) {
							packableLongs = false;
						}
					}
					if (!packableLongs && !allNull) {
						break;
					}
				}
			} else {
				// check for all null
				for (final Object o : b) {
					if (o != null) {
						allNull = false;
						break;
					}
				}
			}
			if (allNull) {
				out.writeByte(Header.ARRAY_OBJECT_ALL_NULL);
				Utils.packInt(out, b.length);
				final Class<?> componentType = obj.getClass().getComponentType();
				serializeClass(out, componentType);
			} else if (packableLongs) {
				out.writeByte(Header.ARRAY_OBJECT_PACKED_LONG);
				out.write(b.length);
				for (final Object o : b) {
					if (o == null) {
						Utils.packLong(out, 0);
					} else {
						Utils.packLong(out, (Long) o + 1);
					}
				}

			} else {
				out.writeByte(Header.ARRAY_OBJECT);
				Utils.packInt(out, b.length);
				// Write classfor components
				final Class<?> componentType = obj.getClass().getComponentType();
				serializeClass(out, componentType);
				for (final Object o : b) {
					serialize(out, o, objectStack);
				}
			}

		} else if (clazz == ArrayList.class) {
			@SuppressWarnings("unchecked")
			final ArrayList<Object> l = (ArrayList<Object>) obj;
			boolean packableLongs = l.size() < 255;
			if (packableLongs) {
				// packable Longs is special case, it is often used in MapDB to
				// reference fields
				for (final Object o : l) {
					if (o != null && (o.getClass() != Long.class || (Long) o < 0 && (Long) o != Long.MAX_VALUE)) {
						packableLongs = false;
						break;
					}
				}
			}
			if (packableLongs) {
				out.writeByte(Header.ARRAYLIST_PACKED_LONG);
				out.write(l.size());
				for (final Object o : l) {
					if (o == null) {
						Utils.packLong(out, 0);
					} else {
						Utils.packLong(out, (Long) o + 1);
					}
				}
			} else {
				serializeCollection(Header.ARRAYLIST, out, obj, objectStack);
			}
		} else if (clazz == LinkedList.class) {
			serializeCollection(Header.LINKEDLIST, out, obj, objectStack);
		} else if (clazz == TreeSet.class) {
			@SuppressWarnings("unchecked")
			final TreeSet<Object> l = (TreeSet<Object>) obj;
			out.writeByte(Header.TREESET);
			Utils.packInt(out, l.size());
			serialize(out, l.comparator(), objectStack);
			for (final Object o : l) {
				serialize(out, o, objectStack);
			}
		} else if (clazz == HashSet.class) {
			serializeCollection(Header.HASHSET, out, obj, objectStack);
		} else if (clazz == LinkedHashSet.class) {
			serializeCollection(Header.LINKEDHASHSET, out, obj, objectStack);
		} else if (clazz == TreeMap.class) {
			@SuppressWarnings("unchecked")
			final TreeMap<Object, Object> l = (TreeMap<Object, Object>) obj;
			out.writeByte(Header.TREEMAP);
			Utils.packInt(out, l.size());
			serialize(out, l.comparator(), objectStack);
			for (final Object o : l.keySet()) {
				serialize(out, o, objectStack);
				serialize(out, l.get(o), objectStack);
			}
		} else if (clazz == HashMap.class) {
			serializeMap(Header.HASHMAP, out, obj, objectStack);
		} else if (clazz == LinkedHashMap.class) {
			serializeMap(Header.LINKEDHASHMAP, out, obj, objectStack);
		} else if (clazz == Properties.class) {
			serializeMap(Header.PROPERTIES, out, obj, objectStack);
		} else {
			serializeUnknownObject(out, obj, objectStack);
		}
	}

	private static void serializeLongArray(final DataOutput out, final long[] val) throws IOException {
		long max = Long.MIN_VALUE;
		long min = Long.MAX_VALUE;
		for (final long i : val) {
			max = Math.max(max, i);
			min = Math.min(min, i);
		}
		if (Byte.MIN_VALUE <= min && max <= Byte.MAX_VALUE) {
			out.writeByte(Header.ARRAY_LONG_BYTE);
			Utils.packInt(out, val.length);
			for (final long i : val) {
				out.write((int) i);
			}
		} else if (Short.MIN_VALUE <= min && max <= Short.MAX_VALUE) {
			out.writeByte(Header.ARRAY_LONG_SHORT);
			Utils.packInt(out, val.length);
			for (final long i : val) {
				out.writeShort((int) i);
			}
		} else if (0 <= min) {
			out.writeByte(Header.ARRAY_LONG_PACKED);
			Utils.packInt(out, val.length);
			for (final long l : val) {
				Utils.packLong(out, l);
			}
		} else if (Integer.MIN_VALUE <= min && max <= Integer.MAX_VALUE) {
			out.writeByte(Header.ARRAY_LONG_INT);
			Utils.packInt(out, val.length);
			for (final long i : val) {
				out.writeInt((int) i);
			}
		} else {
			out.writeByte(Header.ARRAY_LONG);
			Utils.packInt(out, val.length);
			for (final long i : val) {
				out.writeLong(i);
			}
		}
	}

	private static void serializeIntArray(final DataOutput out, final int[] val) throws IOException {
		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;
		for (final int i : val) {
			max = Math.max(max, i);
			min = Math.min(min, i);
		}
		if (Byte.MIN_VALUE <= min && max <= Byte.MAX_VALUE) {
			out.writeByte(Header.ARRAY_INT_BYTE);
			Utils.packInt(out, val.length);
			for (final int i : val) {
				out.writeByte(i);
			}
		} else if (Short.MIN_VALUE <= min && max <= Short.MAX_VALUE) {
			out.writeByte(Header.ARRAY_INT_SHORT);
			Utils.packInt(out, val.length);
			for (final int i : val) {
				out.writeShort(i);
			}
		} else if (0 <= min) {
			out.writeByte(Header.ARRAY_INT_PACKED);
			Utils.packInt(out, val.length);
			for (final int l : val) {
				Utils.packInt(out, l);
			}
		} else {
			out.writeByte(Header.ARRAY_INT);
			Utils.packInt(out, val.length);
			for (final int i : val) {
				out.writeInt(i);
			}
		}
	}

	private void serializeMap(final int header, final DataOutput out, final Object obj, final ReferenceSet<Object> objectStack) throws IOException {
		final Map<?, ?> l = (Map<?, ?>) obj;
		out.writeByte(header);
		Utils.packInt(out, l.size());
		for (final Entry<?, ?> entry : l.entrySet()) {
			serialize(out, entry.getKey(), objectStack);
			serialize(out, entry.getValue(), objectStack);
		}
	}

	private void serializeCollection(final int header, final DataOutput out, final Object obj, final ReferenceSet<Object> objectStack)
			throws IOException {
		final Collection<?> l = (Collection<?>) obj;
		out.writeByte(header);
		Utils.packInt(out, l.size());
		for (final Object o : l) {
			serialize(out, o, objectStack);
		}
	}

	private static void serializeByteArray(final DataOutput out, final byte[] b) throws IOException {
		boolean allEqual = b.length > 0;
		for (int i = 1; i < b.length; i++) {
			if (b[i - 1] != b[i]) {
				allEqual = false;
				break;
			}
		}
		if (allEqual) {
			out.writeByte(Header.ARRAY_BYTE_ALL_EQUAL);
			Utils.packInt(out, b.length);
			out.write(b[0]);
		} else {
			out.writeByte(Header.ARRAY_BYTE);
			Utils.packInt(out, b.length);
			out.write(b);
		}
	}

	protected static String deserializeString(final DataInput din, final int len) throws IOException {
		final char[] b = new char[len];
		for (int i = 0; i < len; i++) {
			b[i] = (char) Utils.unpackInt(din);
		}
		return new String(b);
	}

	@Override
	public Object deserializeObject(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		return deserialize(is, objectStack);
	}

	public Object deserialize(final DataInput is, ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		Object ret = null;
		int ir = 0;
		long lr = 0;
		int head = is.readUnsignedByte();
		while (head == Header.RESET) {
			objectStack.clear();
			head = is.readUnsignedByte();
		}
		/** first try to deserialize object without allocating object stack */
		switch (head) {
		case Header.ZERO_FAIL:
			throw new StreamCorruptedException("Zero Header, data corrupted");
		case Header.NULL:
			return null;
		case Header.BOOLEAN_TRUE:
			ret = Boolean.TRUE;
			break;
		case Header.BOOLEAN_FALSE:
			ret = Boolean.FALSE;
			break;
		case Header.INT_M9:
		case Header.INT_M8:
		case Header.INT_M7:
		case Header.INT_M6:
		case Header.INT_M5:
		case Header.INT_M4:
		case Header.INT_M3:
		case Header.INT_M2:
		case Header.INT_M1:
		case Header.INT_0:
		case Header.INT_1:
		case Header.INT_2:
		case Header.INT_3:
		case Header.INT_4:
		case Header.INT_5:
		case Header.INT_6:
		case Header.INT_7:
		case Header.INT_8:
		case Header.INT_9:
		case Header.INT_10:
		case Header.INT_11:
		case Header.INT_12:
		case Header.INT_13:
		case Header.INT_14:
		case Header.INT_15:
		case Header.INT_16:
			ret = head - Header.INT_M9 - 9;
			break;
		case Header.INT_MIN_VALUE:
			ret = Integer.MIN_VALUE;
			break;
		case Header.INT_MAX_VALUE:
			ret = Integer.MAX_VALUE;
			break;
		case Header.INT_MF3:
		case Header.INT_F3:
			ir = is.readUnsignedByte() & 0xFF;
		case Header.INT_MF2:
		case Header.INT_F2:
			ir = ir << 8 | is.readUnsignedByte() & 0xFF;
		case Header.INT_MF1:
		case Header.INT_F1:
			ir = ir << 8 | is.readUnsignedByte() & 0xFF;
			if (head % 2 == 0) {
				ir = -ir;
			}
			ret = ir;
			break;
		case Header.INT:
			ret = is.readInt();
			break;
		case Header.LONG_M9:
		case Header.LONG_M8:
		case Header.LONG_M7:
		case Header.LONG_M6:
		case Header.LONG_M5:
		case Header.LONG_M4:
		case Header.LONG_M3:
		case Header.LONG_M2:
		case Header.LONG_M1:
		case Header.LONG_0:
		case Header.LONG_1:
		case Header.LONG_2:
		case Header.LONG_3:
		case Header.LONG_4:
		case Header.LONG_5:
		case Header.LONG_6:
		case Header.LONG_7:
		case Header.LONG_8:
		case Header.LONG_9:
		case Header.LONG_10:
		case Header.LONG_11:
		case Header.LONG_12:
		case Header.LONG_13:
		case Header.LONG_14:
		case Header.LONG_15:
		case Header.LONG_16:
			ret = (long) (head - Header.LONG_M9 - 9);
			break;
		case Header.LONG_MIN_VALUE:
			ret = Long.MIN_VALUE;
			break;
		case Header.LONG_MAX_VALUE:
			ret = Long.MAX_VALUE;
			break;

		case Header.LONG_MF7:
		case Header.LONG_F7:
			lr = is.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF6:
		case Header.LONG_F6:
			lr = lr << 8 | is.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF5:
		case Header.LONG_F5:
			lr = lr << 8 | is.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF4:
		case Header.LONG_F4:
			lr = lr << 8 | is.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF3:
		case Header.LONG_F3:
			lr = lr << 8 | is.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF2:
		case Header.LONG_F2:
			lr = lr << 8 | is.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF1:
		case Header.LONG_F1:
			lr = lr << 8 | is.readUnsignedByte() & 0xFFL;
			if (head % 2 == 1) {
				lr = -lr;
			}
			ret = lr;
			break;
		case Header.LONG:
			ret = is.readLong();
			break;
		case Header.BYTE_M1:
			ret = (byte) -1;
			break;
		case Header.BYTE_0:
			ret = (byte) 0;
			break;
		case Header.BYTE_1:
			ret = (byte) 1;
			break;
		case Header.BYTE:
			ret = is.readByte();
			break;
		case Header.CHAR_0:
			ret = (char) 0;
			break;
		case Header.CHAR_1:
			ret = (char) 1;
			break;
		case Header.CHAR_255:
			ret = (char) is.readUnsignedByte();
			break;
		case Header.CHAR:
			ret = is.readChar();
			break;
		case Header.SHORT_M1:
			ret = (short) -1;
			break;
		case Header.SHORT_0:
			ret = (short) 0;
			break;
		case Header.SHORT_1:
			ret = (short) 1;
			break;
		case Header.SHORT_255:
			ret = (short) is.readUnsignedByte();
			break;
		case Header.SHORT_M255:
			ret = (short) -is.readUnsignedByte();
			break;
		case Header.SHORT:
			ret = is.readShort();
			break;
		case Header.FLOAT_M1:
			ret = (float) -1;
			break;
		case Header.FLOAT_0:
			ret = (float) 0;
			break;
		case Header.FLOAT_1:
			ret = (float) 1;
			break;
		case Header.FLOAT_255:
			ret = (float) is.readUnsignedByte();
			break;
		case Header.FLOAT_SHORT:
			ret = (float) is.readShort();
			break;
		case Header.FLOAT:
			ret = is.readFloat();
			break;
		case Header.DOUBLE_M1:
			ret = -1D;
			break;
		case Header.DOUBLE_0:
			ret = 0D;
			break;
		case Header.DOUBLE_1:
			ret = 1D;
			break;
		case Header.DOUBLE_255:
			ret = (double) is.readUnsignedByte();
			break;
		case Header.DOUBLE_SHORT:
			ret = (double) is.readShort();
			break;
		case Header.DOUBLE_INT:
			ret = (double) is.readInt();
			break;
		case Header.DOUBLE:
			ret = is.readDouble();
			break;
		case Header.STRING:
			ret = deserializeString(is, Utils.unpackInt(is));
			break;
		case Header.STRING_0:
			ret = Utils.EMPTY_STRING;
			break;
		case Header.STRING_1:
		case Header.STRING_2:
		case Header.STRING_3:
		case Header.STRING_4:
		case Header.STRING_5:
		case Header.STRING_6:
		case Header.STRING_7:
		case Header.STRING_8:
		case Header.STRING_9:
		case Header.STRING_10:
			if (head - Header.STRING_0 < 0) {
				System.out.println("!");
			}

			ret = deserializeString(is, head - Header.STRING_0);
			break;
		case -1:
			throw new StreamCorruptedException();
		}
		if (ret == null) {
			ret = deserializeArray(head, is);
		}
		if (ret != null) {
			if (objectStack != null) {
				objectStack.add(ret);
			}
			return ret;
		}
		/** something else which needs object stack initialized */
		if (objectStack == null) {
			objectStack = new ReferenceSet<Object>();
		}
		final int oldObjectStackSize = objectStack.getSize();
		switch (head) {
		case Header.OBJECT_STACK:
			final int index = Utils.unpackInt(is);
			ret = objectStack.elementOf(index);
			break;
		case Header.ARRAYLIST:
			ret = deserializeArrayList(is, objectStack);
			break;
		case Header.ARRAY_OBJECT:
			ret = deserializeArrayObject(is, objectStack);
			break;
		case Header.LINKEDLIST:
			ret = deserializeLinkedList(is, objectStack);
			break;
		case Header.TREESET:
			ret = deserializeTreeSet(is, objectStack);
			break;
		case Header.HASHSET:
			ret = deserializeHashSet(is, objectStack);
			break;
		case Header.LINKEDHASHSET:
			ret = deserializeLinkedHashSet(is, objectStack);
			break;
		case Header.TREEMAP:
			ret = deserializeTreeMap(is, objectStack);
			break;
		case Header.HASHMAP:
			ret = deserializeHashMap(is, objectStack);
			break;
		case Header.LINKEDHASHMAP:
			ret = deserializeLinkedHashMap(is, objectStack);
			break;
		case Header.PROPERTIES:
			ret = deserializeProperties(is, objectStack);
			break;
		default:
			ret = deserializeUnknownHeader(is, head, objectStack);
			break;
		}
		if (head != Header.OBJECT_STACK && objectStack.getSize() == oldObjectStackSize) {
			// check if object was not already added to stack as part of collection
			objectStack.add(ret);
		}
		return ret;
	}

	private Object deserializeArray(final int head, final DataInput is) throws IOException, ClassNotFoundException {
		Object ret;
		switch (head) {
		case Header.ARRAY_BYTE_ALL_EQUAL:
			final byte[] b = new byte[Utils.unpackInt(is)];
			Arrays.fill(b, is.readByte());
			ret = b;
			break;
		case Header.ARRAY_BYTE:
			ret = deserializeArrayByte(is);
			break;
		case Header.ARRAY_BOOLEAN:
			ret = readBooleanArray(is);
			break;
		case Header.ARRAY_SHORT:
			int size = Utils.unpackInt(is);
			ret = new short[size];
			final short[] sarray = (short[]) ret;
			for (int i = 0; i < size; i++) {
				sarray[i] = is.readShort();
			}
			break;
		case Header.ARRAY_DOUBLE:
			size = Utils.unpackInt(is);
			ret = new double[size];
			final double[] darray = (double[]) ret;
			for (int i = 0; i < size; i++) {
				darray[i] = is.readDouble();
			}
			break;
		case Header.ARRAY_FLOAT:
			size = Utils.unpackInt(is);
			ret = new float[size];
			final float[] farray = (float[]) ret;
			for (int i = 0; i < size; i++) {
				farray[i] = is.readFloat();
			}
			break;
		case Header.ARRAY_CHAR:
			size = Utils.unpackInt(is);
			ret = new char[size];
			final char[] carray = (char[]) ret;
			for (int i = 0; i < size; i++) {
				carray[i] = is.readChar();
			}
			break;
		case Header.ARRAY_INT_BYTE:
			size = Utils.unpackInt(is);
			ret = new int[size];
			final int[] ibarray = (int[]) ret;
			for (int i = 0; i < size; i++) {
				ibarray[i] = is.readByte();
			}
			break;
		case Header.ARRAY_INT_SHORT:
			size = Utils.unpackInt(is);
			ret = new int[size];
			final int[] isarray = (int[]) ret;
			for (int i = 0; i < size; i++) {
				isarray[i] = is.readShort();
			}
			break;
		case Header.ARRAY_INT_PACKED:
			size = Utils.unpackInt(is);
			ret = new int[size];
			final int[] iparray = (int[]) ret;
			for (int i = 0; i < size; i++) {
				iparray[i] = Utils.unpackInt(is);
			}
			break;
		case Header.ARRAY_INT:
			size = Utils.unpackInt(is);
			ret = new int[size];
			final int[] iarray = (int[]) ret;
			for (int i = 0; i < size; i++) {
				iarray[i] = is.readInt();
			}
			break;
		case Header.ARRAY_LONG_BYTE:
			size = Utils.unpackInt(is);
			ret = new long[size];
			final long[] lbarray = (long[]) ret;
			for (int i = 0; i < size; i++) {
				lbarray[i] = is.readByte();
			}
			break;
		case Header.ARRAY_LONG_SHORT:
			size = Utils.unpackInt(is);
			ret = new long[size];
			final long[] lsarray = (long[]) ret;
			for (int i = 0; i < size; i++) {
				lsarray[i] = is.readShort();
			}
			break;
		case Header.ARRAY_LONG_PACKED:
			size = Utils.unpackInt(is);
			ret = new long[size];
			final long[] lparray = (long[]) ret;
			for (int i = 0; i < size; i++) {
				lparray[i] = Utils.unpackLong(is);
			}
			break;
		case Header.ARRAY_LONG_INT:
			size = Utils.unpackInt(is);
			ret = new long[size];
			final long[] liarray = (long[]) ret;
			for (int i = 0; i < size; i++) {
				liarray[i] = is.readInt();
			}
			break;
		case Header.ARRAY_LONG:
			size = Utils.unpackInt(is);
			ret = new long[size];
			final long[] larray = (long[]) ret;
			for (int i = 0; i < size; i++) {
				larray[i] = is.readLong();
			}
			break;
		case Header.BIGINTEGER:
			ret = new BigInteger(deserializeArrayByte(is));
			break;
		case Header.BIGDECIMAL:
			ret = new BigDecimal(new BigInteger(deserializeArrayByte(is)), Utils.unpackInt(is));
			break;
		case Header.CLASS:
			ret = deserializeClass(is);
			break;
		case Header.DATE:
			ret = new Date(is.readLong());
			break;
		case Header.UUID:
			ret = new UUID(is.readLong(), is.readLong());
			break;
		case Header.ARRAYLIST_PACKED_LONG:
			ret = deserializeArrayListPackedLong(is);
			break;
		case Header.JAVA_SERIALIZATION:
			throw new StreamCorruptedException("Wrong header, data were probably serialized with java.lang.ObjectOutputStream");
		case Header.ARRAY_OBJECT_PACKED_LONG:
			ret = deserializeArrayObjectPackedLong(is);
			break;
		case Header.ARRAY_OBJECT_ALL_NULL:
			ret = deserializeArrayObjectAllNull(is);
			break;
		case Header.ARRAY_OBJECT_NO_REFS:
			ret = deserializeArrayObjectNoRefs(is);
			break;
		default:
			ret = null;
			break;
		}
		return ret;
	}

	private static byte[] deserializeArrayByte(final DataInput is) throws IOException {
		final byte[] bb = new byte[Utils.unpackInt(is)];
		is.readFully(bb);
		return bb;
	}

	private Object[] deserializeArrayObject(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final Class<?> clazz = deserializeClass(is);
		final Object[] s = (Object[]) Array.newInstance(clazz, size);
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			final Object o = deserialize(is, objectStack);
			s[i] = o;
		}
		return s;
	}

	private Object[] deserializeArrayObjectNoRefs(final DataInput is) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final Class<?> clazz = deserializeClass(is);
		final Object[] s = (Object[]) Array.newInstance(clazz, size);
		for (int i = 0; i < size; i++) {
			s[i] = deserialize(is, null);
		}
		return s;
	}

	private Object[] deserializeArrayObjectAllNull(final DataInput is) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final Class<?> clazz = deserializeClass(is);
		final Object[] s = (Object[]) Array.newInstance(clazz, size);
		return s;
	}

	private static Object[] deserializeArrayObjectPackedLong(final DataInput is) throws IOException {
		final int size = is.readUnsignedByte();
		final Object[] s = new Object[size];
		for (int i = 0; i < size; i++) {
			final long l = Utils.unpackLong(is);
			if (l == 0) {
				s[i] = null;
			} else {
				s[i] = l - 1;
			}
		}
		return s;
	}

	private ArrayList<Object> deserializeArrayList(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException,
			ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final ArrayList<Object> s = new ArrayList<Object>(size);
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.add(deserialize(is, objectStack));
		}
		return s;
	}

	private static ArrayList<Object> deserializeArrayListPackedLong(final DataInput is) throws IOException {
		final int size = is.readUnsignedByte();
		if (size < 0) {
			throw new EOFException();
		}

		final ArrayList<Object> s = new ArrayList<Object>(size);
		for (int i = 0; i < size; i++) {
			final long l = Utils.unpackLong(is);
			if (l == 0) {
				s.add(null);
			} else {
				s.add(l - 1);
			}
		}
		return s;
	}

	private LinkedList<?> deserializeLinkedList(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final LinkedList<Object> s = new LinkedList<Object>();
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.add(deserialize(is, objectStack));
		}
		return s;
	}

	private HashSet<Object> deserializeHashSet(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final HashSet<Object> s = new HashSet<Object>(size);
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.add(deserialize(is, objectStack));
		}
		return s;
	}

	private LinkedHashSet<Object> deserializeLinkedHashSet(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException,
			ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final LinkedHashSet<Object> s = new LinkedHashSet<Object>(size);
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.add(deserialize(is, objectStack));
		}
		return s;
	}

	private TreeSet<Object> deserializeTreeSet(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		TreeSet<Object> s = new TreeSet<Object>();
		objectStack.add(s);
		@SuppressWarnings("unchecked")
		final Comparator<Object> comparator = (Comparator<Object>) deserialize(is, objectStack);
		if (comparator != null) {
			s = new TreeSet<Object>(comparator);
		}
		for (int i = 0; i < size; i++) {
			s.add(deserialize(is, objectStack));
		}
		return s;
	}

	private TreeMap<Object, Object> deserializeTreeMap(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException,
			ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		TreeMap<Object, Object> s = new TreeMap<Object, Object>();
		objectStack.add(s);
		@SuppressWarnings("unchecked")
		final Comparator<Object> comparator = (Comparator<Object>) deserialize(is, objectStack);
		if (comparator != null) {
			s = new TreeMap<Object, Object>(comparator);
		}
		for (int i = 0; i < size; i++) {
			s.put(deserialize(is, objectStack), deserialize(is, objectStack));
		}
		return s;
	}

	private HashMap<Object, Object> deserializeHashMap(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException,
			ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final HashMap<Object, Object> s = new HashMap<Object, Object>(size);
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.put(deserialize(is, objectStack), deserialize(is, objectStack));
		}
		return s;
	}

	private LinkedHashMap<Object, Object> deserializeLinkedHashMap(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException,
			ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final LinkedHashMap<Object, Object> s = new LinkedHashMap<Object, Object>(size);
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.put(deserialize(is, objectStack), deserialize(is, objectStack));
		}
		return s;
	}

	private Properties deserializeProperties(final DataInput is, final ReferenceSet<Object> objectStack) throws IOException, ClassNotFoundException {
		final int size = Utils.unpackInt(is);
		final Properties s = new Properties();
		objectStack.add(s);
		for (int i = 0; i < size; i++) {
			s.put(deserialize(is, objectStack), deserialize(is, objectStack));
		}
		return s;
	}

	/** override this method to extend BaseSerializer functionality */
	protected void serializeUnknownObject(final DataOutput out, final Object obj, final ReferenceSet<Object> objectStack)
			throws ObjectStreamException, IOException {
		throw new InternalError("Could not serialize unknown object: " + obj.getClass().getName());
	}

	/** override this method to extend BaseSerializer functionality */
	protected Object deserializeUnknownHeader(final DataInput is, final int head, final ReferenceSet<Object> objectStack)
			throws ObjectStreamException, IOException {
		throw new InternalError("Unknown serialization header: " + head);
	}

	/**
	 * Builds a byte array from the array of booleans, compressing up to 8
	 * booleans per byte.
	 * 
	 * @param bool
	 *           The booleans to be compressed.
	 * @return The fully compressed byte array.
	 */
	protected static byte[] booleanToByteArray(final boolean[] bool) {
		final int boolLen = bool.length;
		final int mod8 = boolLen % 8;
		final byte[] boolBytes = new byte[boolLen / 8 + (boolLen % 8 == 0 ? 0 : 1)];
		final boolean isFlushWith8 = mod8 == 0;
		final int length = isFlushWith8 ? boolBytes.length : boolBytes.length - 1;
		int x = 0;
		int boolByteIndex;
		for (boolByteIndex = 0; boolByteIndex < length;) {
			final byte b = (byte) ((bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2
					| (bool[x++] ? 0x01 : 0x00) << 3 | (bool[x++] ? 0x01 : 0x00) << 4 | (bool[x++] ? 0x01 : 0x00) << 5 | (bool[x++] ? 0x01 : 0x00) << 6 | (bool[x++] ? 0x01
					: 0x00) << 7);
			boolBytes[boolByteIndex++] = b;
		}
		if (!isFlushWith8) {// If length is not a multiple of 8 we must do the
									// last byte conditionally on every element.
			byte b = (byte) 0x00;

			switch (mod8) {
			case 1:
				b |= (bool[x++] ? 0x01 : 0x00) << 0;
				break;
			case 2:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1;
				break;
			case 3:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2;
				break;
			case 4:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2 | (bool[x++] ? 0x01 : 0x00) << 3;
				break;
			case 5:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2 | (bool[x++] ? 0x01 : 0x00) << 3
						| (bool[x++] ? 0x01 : 0x00) << 4;
				break;
			case 6:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2 | (bool[x++] ? 0x01 : 0x00) << 3
						| (bool[x++] ? 0x01 : 0x00) << 4 | (bool[x++] ? 0x01 : 0x00) << 5;
				break;
			case 7:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2 | (bool[x++] ? 0x01 : 0x00) << 3
						| (bool[x++] ? 0x01 : 0x00) << 4 | (bool[x++] ? 0x01 : 0x00) << 5 | (bool[x++] ? 0x01 : 0x00) << 6;
				break;
			case 8:
				b |= (bool[x++] ? 0x01 : 0x00) << 0 | (bool[x++] ? 0x01 : 0x00) << 1 | (bool[x++] ? 0x01 : 0x00) << 2 | (bool[x++] ? 0x01 : 0x00) << 3
						| (bool[x++] ? 0x01 : 0x00) << 4 | (bool[x++] ? 0x01 : 0x00) << 5 | (bool[x++] ? 0x01 : 0x00) << 6 | (bool[x++] ? 0x01 : 0x00) << 7;
				break;
			}
			boolBytes[boolByteIndex++] = b;
		}
		return boolBytes;
	}

	/**
	 * Unpacks an integer from the DataInput indicating the number of booleans
	 * that are compressed. It then calculates the number of bytes, reads them
	 * in, and decompresses and converts them into an array of booleans using the
	 * toBooleanArray(byte[]); method. The array of booleans are trimmed to
	 * <code>numBools</code> elements. This is necessary in situations where the
	 * number of booleans is not a multiple of 8.
	 * 
	 * @return The boolean array decompressed from the bytes read in.
	 * @throws IOException
	 *            If an error occurred while reading.
	 */
	protected static boolean[] readBooleanArray(final DataInput is) throws IOException {
		final int numBools = Utils.unpackInt(is);
		final int length = numBools / 8 + (numBools % 8 == 0 ? 0 : 1);
		final byte[] boolBytes = new byte[length];
		is.readFully(boolBytes);
		final boolean[] tmp = new boolean[boolBytes.length * 8];
		final int len = boolBytes.length;
		int boolIndex = 0;
		for (int x = 0; x < len; x++) {
			for (int y = 0; y < 8; y++) {
				tmp[boolIndex++] = (boolBytes[x] & 0x01 << y) != 0x00;
			}
		}
		// Trim excess booleans
		final boolean[] finalBoolArray = new boolean[numBools];
		System.arraycopy(tmp, 0, finalBoolArray, 0, numBools);
		// Return the trimmed, uncompressed boolean array
		return finalBoolArray;
	}

	protected void serializeClass(final DataOutput out, final Class<?> clazz) throws IOException {
		final String className = clazz.getName();
		serializeString(out, className, null);
	}

	protected Class<?> deserializeClass(final DataInput is) throws IOException, ClassNotFoundException {
		final String s = (String) deserializeObject(is, null);
		return Class.forName(s);
	}

	@Override
	public boolean deserializeBoolean(final DataInput in) throws IOException {
		final int read = in.readUnsignedByte();
		switch (read) {
		case Header.BOOLEAN_TRUE:
			return true;
		case Header.BOOLEAN_FALSE:
			return false;
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeBoolean(final DataOutput out, final boolean b) throws IOException {
		out.writeByte(b ? Header.BOOLEAN_TRUE : Header.BOOLEAN_FALSE);
	}

	@Override
	public void serializeString(final DataOutput out, final String val, final ReferenceSet<Object> rset) throws IOException {
		final int len = val.length();
		if (len == 0) {
			out.writeByte(Header.STRING_0);
		} else {
			if (rset != null) {
				final int index = rset.indexOf(val);
				if (index != -1) {
					out.writeByte(Header.OBJECT_STACK);
					Utils.packInt(out, index);
					return;
				}
				rset.add(val);
			}
			if (len <= 10) {
				out.writeByte(Header.STRING_0 + len);
			} else {
				out.writeByte(Header.STRING);
				Utils.packInt(out, len);
			}
			for (int i = 0; i < len; i++) {
				Utils.packInt(out, val.charAt(i));
			}
		}
	}

	@Override
	public String deserializeString(final DataInput in, final ReferenceSet<Object> rset) throws IOException {
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.OBJECT_STACK:
			final int index = Utils.unpackInt(in);
			return (String) rset.elementOf(index);
		case Header.STRING:
			return deserializeString(in, Utils.unpackInt(in));
		case Header.STRING_0:
			return Utils.EMPTY_STRING;
		case Header.STRING_1:
		case Header.STRING_2:
		case Header.STRING_3:
		case Header.STRING_4:
		case Header.STRING_5:
		case Header.STRING_6:
		case Header.STRING_7:
		case Header.STRING_8:
		case Header.STRING_9:
		case Header.STRING_10:
			return deserializeString(in, header - Header.STRING_0);
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeByte(final DataOutput out, final byte val) throws IOException {
		if (val == -1) {
			out.write(Header.BYTE_M1);
		} else if (val == 0) {
			out.write(Header.BYTE_0);
		} else if (val == 1) {
			out.write(Header.BYTE_1);
		} else {
			out.write(Header.BYTE);
			out.writeByte(val);
		}
	}

	@Override
	public byte deserializeByte(final DataInput in) throws IOException {
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.BYTE_M1:
			return (byte) -1;
		case Header.BYTE_0:
			return (byte) 0;
		case Header.BYTE_1:
			return (byte) 1;
		case Header.BYTE:
			return in.readByte();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeChar(final DataOutput out, final char c) throws IOException {
		if (c == 0) {
			out.write(Header.CHAR_0);
		} else if (c == 1) {
			out.write(Header.CHAR_1);
		} else if (c <= 255) {
			out.write(Header.CHAR_255);
			out.write(c);
		} else {
			out.write(Header.CHAR);
			out.writeChar(c);
		}
	}

	@Override
	public char deserializeChar(final DataInput in) throws IOException {
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.CHAR_0:
			return (char) 0;
		case Header.CHAR_1:
			return (char) 1;
		case Header.CHAR_255:
			return (char) in.readUnsignedByte();
		case Header.CHAR:
			return in.readChar();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeDouble(final DataOutput out, final double val) throws IOException {
		if (val == -1D) {
			out.write(Header.DOUBLE_M1);
		} else if (val == 0D) {
			out.write(Header.DOUBLE_0);
		} else if (val == 1D) {
			out.write(Header.DOUBLE_1);
		} else if (val >= 0 && val <= 255 && (int) val == val) {
			out.write(Header.DOUBLE_255);
			out.write((int) val);
		} else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE && (short) val == val) {
			out.write(Header.DOUBLE_SHORT);
			out.writeShort((short) val);
		} else if (val >= Integer.MIN_VALUE && val <= Integer.MAX_VALUE && (int) val == val) {
			out.write(Header.DOUBLE_INT);
			out.writeInt((int) val);
		} else {
			out.write(Header.DOUBLE);
			out.writeDouble(val);
		}
	}

	@Override
	public double deserializeDouble(final DataInput in) throws IOException {
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.DOUBLE_M1:
			return -1d;
		case Header.DOUBLE_0:
			return 0d;
		case Header.DOUBLE_1:
			return 1d;
		case Header.DOUBLE_255:
			return in.readUnsignedByte();
		case Header.DOUBLE_SHORT:
			return in.readShort();
		case Header.DOUBLE_INT:
			return in.readInt();
		case Header.DOUBLE:
			return in.readDouble();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeFloat(final DataOutput out, final float val) throws IOException {
		if (val == -1f) {
			out.writeByte(Header.FLOAT_M1);
		} else if (val == 0f) {
			out.writeByte(Header.FLOAT_0);
		} else if (val == 1f) {
			out.writeByte(Header.FLOAT_1);
		} else if (val >= 0 && val <= 255 && (int) val == val) {
			out.writeByte(Header.FLOAT_255);
			out.write((int) val);
		} else if (val >= Short.MIN_VALUE && val <= Short.MAX_VALUE && (short) val == val) {
			out.writeByte(Header.FLOAT_SHORT);
			out.writeShort((short) val);
		} else {
			out.writeByte(Header.FLOAT);
			out.writeFloat(val);
		}
	}

	@Override
	public float deserializeFloat(final DataInput in) throws IOException {
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.FLOAT_M1:
			return -1;
		case Header.FLOAT_0:
			return 0;
		case Header.FLOAT_1:
			return 1;
		case Header.FLOAT_255:
			return in.readUnsignedByte();
		case Header.FLOAT_SHORT:
			return in.readShort();
		case Header.FLOAT:
			return in.readFloat();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeShort(final DataOutput out, final short val) throws IOException {
		if (val == -1) {
			out.writeByte(Header.SHORT_M1);
		} else if (val == 0) {
			out.writeByte(Header.SHORT_0);
		} else if (val == 1) {
			out.writeByte(Header.SHORT_1);
		} else if (val > 0 && val < 255) {
			out.writeByte(Header.SHORT_255);
			out.write(val);
		} else if (val < 0 && val > -255) {
			out.writeByte(Header.SHORT_M255);
			out.write(-val);
		} else {
			out.writeByte(Header.SHORT);
			out.writeShort(val);
		}
	}

	@Override
	public short deserializeShort(final DataInput in) throws IOException {
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.SHORT_M1:
			return (short) -1;
		case Header.SHORT_0:
			return (short) 0;
		case Header.SHORT_1:
			return (short) 1;
		case Header.SHORT_255:
			return (short) in.readUnsignedByte();
		case Header.SHORT_M255:
			return (short) -in.readUnsignedByte();
		case Header.SHORT:
			return in.readShort();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeInt(final DataOutput out, int val) throws IOException {
		switch (val) {
		case -9:
		case -8:
		case -7:
		case -6:
		case -5:
		case -4:
		case -3:
		case -2:
		case -1:
		case 0:
		case 1:
		case 2:
		case 3:
		case 4:
		case 5:
		case 6:
		case 7:
		case 8:
		case 9:
		case 10:
		case 11:
		case 12:
		case 13:
		case 14:
		case 15:
		case 16:
			out.writeByte(Header.INT_M9 + val + 9);
			return;
		case Integer.MIN_VALUE:
			out.write(Header.INT_MIN_VALUE);
			return;
		case Integer.MAX_VALUE:
			out.write(Header.INT_MAX_VALUE);
			return;
		}
		if ((Math.abs(val) >>> 24 & 0xFF) != 0) {
			out.write(Header.INT);
			out.writeInt(val);
			return;
		}
		int neg = 0;
		if (val < 0) {
			neg = -1;
			val = -val;
		}
		int size = 24;
		while ((val >> size & 0xFFL) == 0) {
			size -= 8;
		}
		out.write(Header.INT_F1 + size / 8 * 2 + neg);
		while (size >= 0) {
			out.write((int) (val >> size & 0xFFL));
			size -= 8;
		}
	}

	@Override
	public int deserializeInt(final DataInput in) throws IOException {
		int ir = 0;
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.INT_M9:
		case Header.INT_M8:
		case Header.INT_M7:
		case Header.INT_M6:
		case Header.INT_M5:
		case Header.INT_M4:
		case Header.INT_M3:
		case Header.INT_M2:
		case Header.INT_M1:
		case Header.INT_0:
		case Header.INT_1:
		case Header.INT_2:
		case Header.INT_3:
		case Header.INT_4:
		case Header.INT_5:
		case Header.INT_6:
		case Header.INT_7:
		case Header.INT_8:
		case Header.INT_9:
		case Header.INT_10:
		case Header.INT_11:
		case Header.INT_12:
		case Header.INT_13:
		case Header.INT_14:
		case Header.INT_15:
		case Header.INT_16:
			return header - Header.INT_M9 - 9;
		case Header.INT_MIN_VALUE:
			return Integer.MIN_VALUE;
		case Header.INT_MAX_VALUE:
			return Integer.MAX_VALUE;
		case Header.INT_MF3:
		case Header.INT_F3:
			ir = in.readUnsignedByte() & 0xFF;
		case Header.INT_MF2:
		case Header.INT_F2:
			ir = ir << 8 | in.readUnsignedByte() & 0xFF;
		case Header.INT_MF1:
		case Header.INT_F1:
			ir = ir << 8 | in.readUnsignedByte() & 0xFF;
			if (header % 2 == 0) {
				ir = -ir;
			}
			return ir;
		case Header.INT:
			return in.readInt();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void serializeLong(final DataOutput out, long val) throws IOException {
		if (val >= -9 && val <= 16) {
			out.write((int) (Header.LONG_M9 + (val + 9)));
			return;
		} else if (val == Long.MIN_VALUE) {
			out.write(Header.LONG_MIN_VALUE);
			return;
		} else if (val == Long.MAX_VALUE) {
			out.write(Header.LONG_MAX_VALUE);
			return;
		} else if ((Math.abs(val) >>> 56 & 0xFF) != 0) {
			out.write(Header.LONG);
			out.writeLong(val);
			return;
		}
		int neg = 0;
		if (val < 0) {
			neg = -1;
			val = -val;
		}
		// calculate N bytes
		int size = 48;
		while ((val >> size & 0xFFL) == 0) {
			size -= 8;
		}
		// write header
		out.write(Header.LONG_F1 + size / 8 * 2 + neg);
		// write data
		while (size >= 0) {
			out.write((int) (val >> size & 0xFFL));
			size -= 8;
		}
	}

	@Override
	public long deserializeLong(final DataInput in) throws IOException {
		long lr = 0;
		final int header = in.readUnsignedByte();
		switch (header) {
		case Header.LONG_M9:
		case Header.LONG_M8:
		case Header.LONG_M7:
		case Header.LONG_M6:
		case Header.LONG_M5:
		case Header.LONG_M4:
		case Header.LONG_M3:
		case Header.LONG_M2:
		case Header.LONG_M1:
		case Header.LONG_0:
		case Header.LONG_1:
		case Header.LONG_2:
		case Header.LONG_3:
		case Header.LONG_4:
		case Header.LONG_5:
		case Header.LONG_6:
		case Header.LONG_7:
		case Header.LONG_8:
		case Header.LONG_9:
		case Header.LONG_10:
		case Header.LONG_11:
		case Header.LONG_12:
		case Header.LONG_13:
		case Header.LONG_14:
		case Header.LONG_15:
		case Header.LONG_16:
			return header - Header.LONG_M9 - 9;
		case Header.LONG_MIN_VALUE:
			return Long.MIN_VALUE;
		case Header.LONG_MAX_VALUE:
			return Long.MAX_VALUE;
		case Header.LONG_MF7:
		case Header.LONG_F7:
			lr = in.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF6:
		case Header.LONG_F6:
			lr = lr << 8 | in.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF5:
		case Header.LONG_F5:
			lr = lr << 8 | in.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF4:
		case Header.LONG_F4:
			lr = lr << 8 | in.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF3:
		case Header.LONG_F3:
			lr = lr << 8 | in.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF2:
		case Header.LONG_F2:
			lr = lr << 8 | in.readUnsignedByte() & 0xFFL;
		case Header.LONG_MF1:
		case Header.LONG_F1:
			lr = lr << 8 | in.readUnsignedByte() & 0xFFL;
			if (header % 2 == 1) {
				lr = -lr;
			}
			return lr;
		case Header.LONG:
			return in.readLong();
		default:
			throw new StreamCorruptedException();
		}
	}

	@Override
	public void reset(final DataOutput out, final ReferenceSet<Object> rset) throws IOException {
		if (rset != null) {
			rset.clear();
		}
		out.writeByte(Header.RESET);
	}
}
