package com.bzsoft.ser;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

public class ObjectInputStream extends InputStream implements ObjectInput {

	private final Serializer<Object>		serializer;
	private final DataInputStream			din;
	private final ReferenceSet<Object>	rset;

	public ObjectInputStream(final InputStream is) {
		serializer = new BeanSerializer();
		din = new DataInputStream(is);
		rset = new ReferenceSet<Object>();
	}

	protected ObjectInputStream(final DataInputStream is, final Serializer<Object> ser, final ReferenceSet<Object> rfset) {
		rset = rfset;
		serializer = ser;
		din = is;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean readBoolean() throws IOException {
		return serializer.deserializeBoolean(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte readByte() throws IOException {
		return serializer.deserializeByte(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public char readChar() throws IOException {
		return serializer.deserializeChar(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public double readDouble() throws IOException {
		return serializer.deserializeDouble(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public float readFloat() throws IOException {
		return serializer.deserializeFloat(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFully(final byte[] abyte0) throws IOException {
		din.readFully(abyte0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void readFully(final byte[] abyte0, final int i, final int j) throws IOException {
		din.readFully(abyte0, i, j);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readInt() throws IOException {
		return serializer.deserializeInt(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String readLine() throws IOException {
		return serializer.deserializeString(din, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long readLong() throws IOException {
		return serializer.deserializeLong(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public short readShort() throws IOException {
		return serializer.deserializeShort(din);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String readUTF() throws IOException {
		return serializer.deserializeString(din, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readUnsignedByte() throws IOException {
		return din.readUnsignedByte();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int readUnsignedShort() throws IOException {
		return din.readUnsignedShort();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int skipBytes(final int i) throws IOException {
		return din.skipBytes(i);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object readObject() throws ClassNotFoundException, IOException {
		return serializer.deserializeObject(din, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {
		return din.read();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		din.close();
	}
}
