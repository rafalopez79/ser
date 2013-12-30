package com.bzsoft.ser;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

public class ObjectOutputStream extends OutputStream implements ObjectOutput {

	private final Serializer<Object>		serializer;
	private final DataOutputStream		dout;
	private final ReferenceSet<Object>	rset;

	public ObjectOutputStream(final OutputStream os) {
		rset = new ReferenceSet<Object>();
		serializer = new BeanSerializer();
		dout = new DataOutputStream(os);
	}

	protected ObjectOutputStream(final DataOutputStream os, final Serializer<Object> ser, final ReferenceSet<Object> rfset) {
		rset = rfset;
		serializer = ser;
		dout = os;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeBoolean(final boolean v) throws IOException {
		serializer.serializeBoolean(dout, v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeByte(final int v) throws IOException {
		serializer.serializeByte(dout, (byte) v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeBytes(final String s) throws IOException {
		serializer.serializeString(dout, s, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeChar(final int v) throws IOException {
		serializer.serializeChar(dout, (char) v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeChars(final String s) throws IOException {
		serializer.serializeString(dout, s, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeDouble(final double v) throws IOException {
		serializer.serializeDouble(dout, v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeFloat(final float v) throws IOException {
		serializer.serializeFloat(dout, v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeInt(final int v) throws IOException {
		serializer.serializeInt(dout, v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeLong(final long v) throws IOException {
		serializer.serializeLong(dout, v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeShort(final int v) throws IOException {
		serializer.serializeShort(dout, (short) v);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeUTF(final String s) throws IOException {
		serializer.serializeString(dout, s, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void writeObject(final Object o) throws IOException {
		serializer.serializeObject(dout, o, rset);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final int b) throws IOException {
		dout.write(b);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		dout.close();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException {
		dout.flush();
	}

	public void reset() throws IOException {
		serializer.reset(dout, rset);
	}
}
