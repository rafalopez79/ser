package com.bzsoft.ser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;

public class Test implements Serializable {

	private static interface Inter {
		String getName();

		@Override
		public int hashCode();

		@Override
		public boolean equals(Object obj);
	}

	private static class InterImpl implements Inter {

		String	a;

		public InterImpl() {
			//
		}

		public InterImpl(final String s) {
			a = s;
		}

		@Override
		public String getName() {
			return a;
		}

		@Override
		public String toString() {
			return "InterImpl [a=" + a + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (a == null ? 0 : a.hashCode());
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final InterImpl other = (InterImpl) obj;
			if (a == null) {
				if (other.a != null) {
					return false;
				}
			} else if (!a.equals(other.a)) {
				return false;
			}
			return true;
		}

	}

	private static class InterImplB implements Inter, Externalizable {
		// final double[] dd = { 0, 0.1, 0.2, 0.3 };
		Object[]	o	= { null, 1, 3.0 };
		String	a;

		public InterImplB() {
			//
		}

		public InterImplB(final String s) {
			a = s;
		}

		@Override
		public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
			o = (Object[]) in.readObject();
			a = (String) in.readObject();
		}

		@Override
		public void writeExternal(final ObjectOutput out) throws IOException {
			out.writeObject(o);
			out.writeObject(a);
		}

		@Override
		public String getName() {
			return a;
		}

		@Override
		public String toString() {
			return "InterImplB [a=" + a + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (a == null ? 0 : a.hashCode());
			result = prime * result + Arrays.hashCode(o);
			return result;
		}

		@Override
		public boolean equals(final Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final InterImplB other = (InterImplB) obj;
			if (a == null) {
				if (other.a != null) {
					return false;
				}
			} else if (!a.equals(other.a)) {
				return false;
			}
			if (!Arrays.equals(o, other.o)) {
				return false;
			}
			return true;
		}

	}

	final Object[]				o;
	private final Inter[]	aaa;
	private final Integer[]	array;

	/**
     *
     */
	public Test() {
		o = new Object[50];
		int j = 0;
		for (int i = 0; i < o.length; i++) {
			if (i % 3 == 0) {
				o[i] = (double) j;
			} else {
				o[i] = 1;
			}
			j++;
		}
		array = new Integer[50];
		for (int i = 0; i < array.length; i++) {
			if (i % 3 == 0) {
				array[i] = null;
			} else if (i % 3 == 1) {
				array[i] = i;
			} else {
				array[i] = 1;
			}
		}
		aaa = new Inter[5000];
		for (int i = 0; i < aaa.length; i++) {
			aaa[i] = new InterImplB(String.valueOf(i));
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(array);
		result = prime * result + Arrays.hashCode(o);
		result = prime * result + Arrays.hashCode(aaa);
		return result;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final Test other = (Test) obj;
		if (!Arrays.equals(aaa, other.aaa)) {
			return false;
		}
		if (!Arrays.equals(array, other.array)) {
			return false;
		}
		if (!Arrays.equals(o, other.o)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		// return "Test [aa=" + aa + ", array=" + Arrays.toString(array) + "]";
		return "test";
	}

	public static void main(final String[] args) throws Exception {
		final byte[] b = { 1, 2 };
		System.out.println(b.getClass() == byte[].class);
		System.out.println("me");
		final ByteArrayOutputStream baos = new ByteArrayOutputStream(200 * 1024 * 1024);
		final ObjectOutputStream os = new ObjectOutputStream(baos);
		final long ini = System.currentTimeMillis();
		final int count = 200;
		final Test[] array = new Test[count];
		for (int i = 0; i < count; i++) {
			final Test t = new Test();
			os.writeObject(t);
			os.reset();
			array[i] = t;
		}
		os.close();
		// System.out.println(new String(baos.toByteArray()));
		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final ObjectInputStream is = new ObjectInputStream(bais);
		for (int i = 0; i < count; i++) {
			final Test t = (Test) is.readObject();
			if (!t.equals(array[i])) {
				System.err.println("Error!");
			}
		}
		is.close();
		final long end = System.currentTimeMillis();
		final int size = baos.toByteArray().length;
		System.out.println("me time " + (end - ini) + " ms");
		System.out.println(size + " bytes");
		System.out.println((double) size / (end - ini) + " kbs");

		System.out.println("java");

		final ByteArrayOutputStream baos2 = new ByteArrayOutputStream(200 * 1024 * 1024);
		final java.io.ObjectOutputStream os2 = new java.io.ObjectOutputStream(baos2);
		final long ini2 = System.currentTimeMillis();
		final Test[] array2 = new Test[count];
		for (int i = 0; i < count; i++) {
			final Test t = new Test();
			os2.writeObject(t);
			os2.reset();
			array2[i] = t;
		}
		os2.close();
		// System.out.println(new String(baos.toByteArray()));
		final ByteArrayInputStream bais2 = new ByteArrayInputStream(baos2.toByteArray());
		final java.io.ObjectInputStream is2 = new java.io.ObjectInputStream(bais2);
		for (int i = 0; i < count; i++) {
			final Test t = (Test) is2.readObject();
			if (!t.equals(array2[i])) {
				System.err.println("Error!");
			}
		}
		is.close();
		final long end2 = System.currentTimeMillis();
		final int size2 = baos2.toByteArray().length;
		System.out.println("java time " + (end2 - ini2) + " ms");
		System.out.println(size2 + " bytes");
		System.out.println((double) size2 / (end2 - ini2) + " kbs");

		// System.out.println("Equals " + t1.equals(t2));
		// final boolean eq = deepCompare(t1, t2);
		// final boolean eq1 = deepCompare(t11, t22);
		// final boolean eq2 = deepCompare(t111, t222);
		// System.out.println("Deep Equals " + eq);
	}

	public static boolean deepCompare(final Object a, final Object b) {
		final ByteArrayOutputStream baosa = new ByteArrayOutputStream();
		final ByteArrayOutputStream baosb = new ByteArrayOutputStream();
		try {
			final java.io.ObjectOutputStream oosa = new java.io.ObjectOutputStream(baosa);
			final java.io.ObjectOutputStream oosb = new java.io.ObjectOutputStream(baosb);
			final long ini = System.currentTimeMillis();
			oosa.writeObject(a);
			final long end = System.currentTimeMillis();
			oosb.writeObject(b);
			oosa.close();
			oosb.close();
			// System.out.println(Utils.toHex(baosa.toByteArray()));
			// System.out.println(Utils.toHex(baosb.toByteArray()));
			System.out.println("java length " + baosa.toByteArray().length + " bytes");
			System.out.println("java time " + (end - ini) + " ms");
			return Arrays.equals(baosa.toByteArray(), baosb.toByteArray());
		} catch (final Exception e) {
			return false;
		}
	}
}
