package com.bzsoft.ser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Serializer<A> {

	public void serializeObject(DataOutput out, A value, ReferenceSet<A> rset) throws IOException;

	public void serializeBoolean(DataOutput out, boolean b) throws IOException;

	public void serializeString(DataOutput out, String s, ReferenceSet<A> rset) throws IOException;

	public void serializeByte(DataOutput out, byte b) throws IOException;

	public void serializeChar(DataOutput out, char c) throws IOException;

	public void serializeDouble(DataOutput out, double d) throws IOException;

	public void serializeFloat(DataOutput out, float f) throws IOException;

	public void serializeShort(DataOutput out, short s) throws IOException;

	public void serializeInt(DataOutput out, int i) throws IOException;

	public void serializeLong(DataOutput out, long i) throws IOException;

	public A deserializeObject(DataInput in, ReferenceSet<Object> rset) throws IOException, ClassNotFoundException;

	public boolean deserializeBoolean(DataInput in) throws IOException;

	public String deserializeString(DataInput in, ReferenceSet<A> rset) throws IOException;

	public byte deserializeByte(DataInput in) throws IOException;

	public char deserializeChar(DataInput in) throws IOException;

	public double deserializeDouble(DataInput in) throws IOException;

	public float deserializeFloat(DataInput in) throws IOException;

	public short deserializeShort(DataInput in) throws IOException;

	public int deserializeInt(DataInput in) throws IOException;

	public long deserializeLong(DataInput in) throws IOException;

	public void reset(DataOutput in, ReferenceSet<A> rset) throws IOException;

}
