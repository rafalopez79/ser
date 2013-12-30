package com.bzsoft.ser;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.InvalidObjectException;
import java.io.NotSerializableException;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class BeanSerializer extends BaseSerializer {

	protected static final class SpecialSerializationInfo {
		private final Method	readObject;
		private final Method	writeObject;
		private final Method	readObjectNoData;

		protected SpecialSerializationInfo(final Method readObject, final Method writeObject, final Method readObjectNoData) {
			this.readObject = readObject;
			this.writeObject = writeObject;
			this.readObjectNoData = readObjectNoData;
		}

		public Method getReadObject() {
			return readObject;
		}

		public Method getWriteObject() {
			return writeObject;
		}

		public Method getReadObjectNoData() {
			return readObjectNoData;
		}

	}

	protected static final class FieldInfo implements Comparable<FieldInfo> {
		private final String	name;
		private final Field	field;

		protected FieldInfo(final String name, final Class<?> clazz) throws IOException {
			this.name = name;
			Class<?> aClazz = clazz;
			Field field;
			// iterate over class hierarchy, until root class
			while (true) {
				if (aClazz == Object.class) {
					throw new InvalidObjectException("Could not set field value: " + name + " - " + clazz.toString());
				}
				// access field directly
				try {
					final Field f = aClazz.getDeclaredField(name);
					// security manager may not be happy about this
					if (!f.isAccessible()) {
						f.setAccessible(true);
					}
					field = f;
					break;
				} catch (final NoSuchFieldException e) {
					// field does not exists
				}
				// move to superclass
				aClazz = aClazz.getSuperclass();
			}
			this.field = field;
		}

		public String getName() {
			return name;
		}

		public Field getField() {
			return field;
		}

		@Override
		public int hashCode() {
			return field.hashCode();
		}

		@Override
		public int compareTo(final FieldInfo o) {
			return name.compareTo(o.name);
		}

		@Override
		public boolean equals(final Object obj) {
			return field.equals(obj);
		}

		@Override
		public String toString() {
			return field.toString();
		}
	}

	protected static final class ClassInfo {

		private final String											name;
		private final List<FieldInfo>								fields;
		private final Map<Class<?>, Map<String, Integer>>	mClassNameID;
		private final boolean										isEnum;
		private final boolean										externalizable;
		private final SpecialSerializationInfo					ssi;

		protected ClassInfo(final String name, final FieldInfo[] fields, final boolean isEnum, final boolean isExternalizable,
				final SpecialSerializationInfo ssi) {
			this.fields = new ArrayList<FieldInfo>();
			this.mClassNameID = new HashMap<Class<?>, Map<String, Integer>>();
			this.name = name;
			this.isEnum = isEnum;
			externalizable = isExternalizable;
			this.ssi = ssi;
			for (final FieldInfo f : fields) {
				final int index = this.fields.size();
				final Class<?> clazz = f.getField().getDeclaringClass();
				Map<String, Integer> map = mClassNameID.get(clazz);
				if (map == null) {
					map = new HashMap<String, Integer>();
					mClassNameID.put(clazz, map);
				}
				map.put(f.getName(), index);
				this.fields.add(f);
			}
		}

		public String getName() {
			return name;
		}

		public FieldInfo[] getFields() {
			return fields.toArray(new FieldInfo[fields.size()]);
		}

		public boolean isEnum() {
			return isEnum;
		}

		public boolean isExternalizable() {
			return externalizable;
		}

		public SpecialSerializationInfo getSsi() {
			return ssi;
		}

		public int getFieldId(final Class<?> clazz, final String name) {
			final Map<String, Integer> mName = mClassNameID.get(clazz);
			if (mName == null) {
				return -1;
			}
			final Integer fieldId = mName.get(name);
			if (fieldId != null) {
				return fieldId;
			}
			return -1;
		}

		public FieldInfo getField(final int serialId) {
			return fields.get(serialId);
		}

		public int addFieldInfo(final FieldInfo field) {
			final int index = fields.size();
			final Class<?> clazz = field.getField().getDeclaringClass();
			Map<String, Integer> map = mClassNameID.get(clazz);
			if (map == null) {
				map = new HashMap<String, Integer>();
				mClassNameID.put(clazz, map);
			}
			map.put(field.getName(), index);
			fields.add(field);
			return index;
		}

		@Override
		public String toString() {
			return super.toString() + "[" + getName() + "]";
		}
	}

	protected static final Method									sunConstructor;
	protected static final Object									sunReflFac;
	protected static final Map<Class<?>, Constructor<?>>	class2constuctor;

	static {
		Method sc = null;
		Object srf = null;
		try {
			final Class<?> clazz = Class.forName("sun.reflect.ReflectionFactory");
			if (clazz != null) {
				final Method getReflectionFactory = clazz.getMethod("getReflectionFactory");
				srf = getReflectionFactory.invoke(null);
				sc = clazz.getMethod("newConstructorForSerialization", java.lang.Class.class, java.lang.reflect.Constructor.class);
			} else {
				srf = null;
				sc = null;
			}
		} catch (final Exception e) {
			// ignore
		}
		class2constuctor = new IdentityHashMap<Class<?>, Constructor<?>>();
		sunConstructor = sc;
		sunReflFac = srf;
	}

	protected final List<ClassInfo>								registered;
	protected final Map<Class<?>, Integer>						class2classId;
	protected final Map<Integer, Class<?>>						classId2class;

	public BeanSerializer() {
		registered = new ArrayList<ClassInfo>();
		class2classId = new HashMap<Class<?>, Integer>();
		classId2class = new HashMap<Integer, Class<?>>();
	}

	private final static List<FieldInfo> getFieldInfo(final Class<?> clazz) throws IOException {
		final List<FieldInfo> list = new ArrayList<FieldInfo>();
		Class<?> aClazz = clazz;
		while (true) {
			if (aClazz == null || aClazz == Object.class) {
				break;
			}
			final List<FieldInfo> fiList = new ArrayList<FieldInfo>();
			final Field[] fields = aClazz.getDeclaredFields();
			for (final Field field : fields) {
				final int mod = field.getModifiers();
				if (Modifier.isTransient(mod) || Modifier.isStatic(mod)) {
					continue;
				}
				fiList.add(new FieldInfo(field.getName(), aClazz));
			}
			Collections.sort(fiList);
			list.addAll(fiList);
			aClazz = aClazz.getSuperclass();
		}
		return list;
	}

	protected int registerClass(final Class<?> clazz) throws IOException {
		if (clazz != Object.class) {
			assertClassSerializable(clazz);
		}
		if (containsClass(clazz)) {
			return class2classId.get(clazz);
		}
		final boolean externalizable = isExternalizable(clazz);
		final SpecialSerializationInfo ssi = getSpecialSerializationInfo(clazz);
		final List<FieldInfo> fiList = externalizable || ssi != null ? Collections.<FieldInfo> emptyList() : getFieldInfo(clazz);
		final ClassInfo info = new ClassInfo(clazz.getName(), fiList.toArray(new FieldInfo[fiList.size()]), clazz.isEnum(), externalizable, ssi);
		final int id = registered.size();
		class2classId.put(clazz, id);
		classId2class.put(id, clazz);
		registered.add(info);
		return id;
	}

	protected static boolean isExternalizable(final Class<?> clazz) {
		return Externalizable.class.isAssignableFrom(clazz);
	}

	protected static SpecialSerializationInfo getSpecialSerializationInfo(final Class<?> clazz) {
		Method readObject = null;
		Method writeObject = null;
		Method readObjectNoData = null;
		try {
			readObject = clazz.getDeclaredMethod("readObject", ObjectInputStream.class);
		} catch (final NoSuchMethodException e) {
			// empty
		}
		try {
			writeObject = clazz.getDeclaredMethod("writeObject", ObjectOutputStream.class);
		} catch (final NoSuchMethodException e) {
			// empty
		}
		try {
			readObjectNoData = clazz.getDeclaredMethod("readObjectNoData");
		} catch (final NoSuchMethodException e) {
			// empty
		}
		if (readObjectNoData != null && readObject != null && writeObject != null) {
			readObject.setAccessible(true);
			readObjectNoData.setAccessible(true);
			writeObject.setAccessible(true);
			return new SpecialSerializationInfo(readObject, writeObject, readObjectNoData);
		}
		return null;
	}

	protected FieldInfo[] getFields(final Class<?> clazz) throws IOException {
		FieldInfo[] fields = null;
		ClassInfo classInfo = null;
		final Integer classId = class2classId.get(clazz);
		if (classId != null) {
			classInfo = registered.get(classId);
			fields = classInfo.getFields();
		} else {
			throw new StreamCorruptedException();
		}
		return fields;
	}

	protected void assertClassSerializable(final Class<?> clazz) throws NotSerializableException, InvalidClassException {
		if (containsClass(clazz)) {
			return;
		}
		if (!clazz.isInterface() && !Serializable.class.isAssignableFrom(clazz)) {
			throw new NotSerializableException(clazz.getName());
		}
	}

	public Object getFieldValue(final FieldInfo fieldInfo, final Object object) throws IllegalAccessException {
		if (fieldInfo.field == null) {
			throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.getName());
		}
		return fieldInfo.field.get(object);
	}

	public void setFieldValue(final FieldInfo fieldInfo, final Object object, final Object value) throws IllegalAccessException {
		if (fieldInfo.field == null) {
			throw new NoSuchFieldError(object.getClass() + "." + fieldInfo.getName());
		}
		fieldInfo.field.set(object, value);
	}

	public boolean containsClass(final Class<?> clazz) {
		return class2classId.containsKey(clazz);
	}

	public Integer getClassId(final Class<?> clazz) {
		return class2classId.get(clazz);
	}

	@Override
	protected void serializeClass(final DataOutput out, final Class<?> clazz) throws IOException {
		Integer classId = getClassId(clazz);
		if (classId != null) {
			out.write(Header.CLASS_ID);
			Utils.packInt(out, classId);
		} else {
			out.write(Header.CLASS_NID);
			classId = registerClass(clazz);
			super.serializeClass(out, clazz);
		}
	}

	@Override
	protected Class<?> deserializeClass(final DataInput in) throws IOException, ClassNotFoundException {
		final int type = in.readUnsignedByte();
		final int classId;
		if (type == Header.CLASS_ID) {
			classId = Utils.unpackInt(in);
		} else if (type == Header.CLASS_NID) {
			final Class<?> clazz = super.deserializeClass(in);
			classId = registerClass(clazz);
		} else {
			throw new StreamCorruptedException();
		}
		return classId2class.get(classId);
	}

	@Override
	protected void serializeUnknownObject(final DataOutput out, final Object obj, final ReferenceSet<Object> objectStack) throws IOException {
		out.write(Header.BEAN);
		// write class header
		final Class<?> clazz = obj.getClass();
		serializeClass(out, clazz);
		final Integer classId = getClassId(clazz);
		final ClassInfo classInfo = registered.get(classId);
		final SpecialSerializationInfo ssi = classInfo.getSsi();
		if (classInfo.isEnum()) {
			final int ordinal = ((Enum<?>) obj).ordinal();
			Utils.packInt(out, ordinal);
		} else if (classInfo.isExternalizable()) {
			final ObjectOutputStream os = new ObjectOutputStream((DataOutputStream) out, this, objectStack);
			((Externalizable) obj).writeExternal(os);
			os.close();
			return;
		} else if (ssi != null) {
			final ObjectOutputStream os = new ObjectOutputStream((DataOutputStream) out, this, objectStack);
			try {
				ssi.getWriteObject().invoke(obj, os);
			} catch (final Exception e) {
				throw new StreamCorruptedException();
			}
			os.close();
		} else {
			final FieldInfo[] fields = getFields(obj.getClass());
			Utils.packInt(out, fields.length);
			try {
				for (final FieldInfo f : fields) {
					// write field ID
					final int fieldId = classInfo.getFieldId(f.getField().getDeclaringClass(), f.getName());
					Utils.packInt(out, fieldId);
					// and write value
					final Object fieldValue = getFieldValue(classInfo.getField(fieldId), obj);
					serialize(out, fieldValue, objectStack);
				}
			} catch (final IllegalAccessException e) {
				throw new StreamCorruptedException();
			}
		}
	}

	@Override
	protected Object deserializeUnknownHeader(final DataInput in, final int head, final ReferenceSet<Object> objectStack) throws IOException {
		if (head != Header.BEAN) {
			throw new StreamCorruptedException();
		}
		// read class header
		try {
			final Class<?> clazz = deserializeClass(in);
			final Integer classId = class2classId.get(clazz);
			final ClassInfo classInfo = registered.get(classId);
			assertClassSerializable(clazz);
			final SpecialSerializationInfo ssi = classInfo.getSsi();
			Object o;
			if (classInfo.isEnum) {
				final int ordinal = Utils.unpackInt(in);
				o = clazz.getEnumConstants()[ordinal];
			} else {
				o = createInstanceSkippingConstructor(clazz);
			}
			objectStack.add(o);
			if (classInfo.isExternalizable()) {
				final ObjectInputStream in2 = new ObjectInputStream((DataInputStream) in, this, objectStack);
				((Externalizable) o).readExternal(in2);
				in2.close();
			} else if (ssi != null) {
				final ObjectInputStream in2 = new ObjectInputStream((DataInputStream) in, this, objectStack);
				ssi.getReadObject().invoke(o, in2);
				in2.close();
			} else {
				final int fieldCount = Utils.unpackInt(in);
				for (int i = 0; i < fieldCount; i++) {
					final int fieldId = Utils.unpackInt(in);
					final FieldInfo f = classInfo.getField(fieldId);
					final Object fieldValue = deserialize(in, objectStack);
					setFieldValue(f, o, fieldValue);
				}
			}
			return o;
		} catch (final Exception e) {
			e.printStackTrace();
			throw new StreamCorruptedException(e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	protected <T> T createInstanceSkippingConstructor(final Class<T> clazz) throws NoSuchMethodException, InvocationTargetException,
			IllegalAccessException, InstantiationException {
		if (sunConstructor != null) {
			// Sun specific way
			Constructor<?> intConstr = class2constuctor.get(clazz);
			if (intConstr == null) {
				final Constructor<?> objDef = Object.class.getDeclaredConstructor();
				intConstr = (Constructor<?>) sunConstructor.invoke(sunReflFac, clazz, objDef);
				class2constuctor.put(clazz, intConstr);
			}
			return (T) intConstr.newInstance();
		}
		// try usual generic stuff which does not skip constructor
		Constructor<?> c = class2constuctor.get(clazz);
		if (c == null) {
			c = clazz.getConstructor();
			if (!c.isAccessible()) {
				c.setAccessible(true);
			}
			class2constuctor.put(clazz, c);
		}
		return (T) c.newInstance();
	}
}