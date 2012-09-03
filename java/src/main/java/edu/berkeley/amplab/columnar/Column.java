package edu.berkeley.amplab.columnar;

import java.nio.ByteBuffer;
import java.util.Iterator;

import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

/**
 * A collection of basic columns.
 */
public abstract class Column {

  /**
   * Initialize a new column for use.
   * 
   * @param initialCapacity
   *          The initial capacity to allocate.
   */
  public abstract void initialize(int initialCapacity);

  /**
   * Reconstruct a column from the binary data in the byte buffer.
   * 
   * @param byteBuffers
   */
  public abstract void initialize(Iterator<ByteBuffer> byteBuffers);

  /**
   * Serialize the column into an array of ByteBuffer.
   * 
   * @return an array of ByteBuffer serializing this column.
   */
  public abstract ByteBuffer[] asByteBuffers();

  /**
   * Primitive integer column.
   */
  public static class IntColumn extends Column {
    private IntArrayList arr;

    @Override
    public void initialize(int initialCapacity) {
      arr = new IntArrayList(initialCapacity);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new IntArrayList(byteBuffers.next().asIntBuffer().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      ByteBuffer b = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE * arr.size());
      b.asIntBuffer().put(arr.elements());
      return new ByteBuffer[] { b };
    }

    public int get(int index) {
      return arr.get(index);
    }

    public void add(int v) {
      arr.add(v);
    }
  }

  /**
   * Primitive long column.
   */
  public static class LongColumn extends Column {
    private LongArrayList arr;

    @Override
    public void initialize(int initialCapacity) {
      arr = new LongArrayList(initialCapacity);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new LongArrayList(byteBuffers.next().asLongBuffer().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      ByteBuffer b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * arr.size());
      b.asLongBuffer().put(arr.elements());
      return new ByteBuffer[] { b };
    }

    public long get(int index) {
      return arr.get(index);
    }

    public void add(long v) {
      arr.add(v);
    }
  }

  /**
   * Primitive float column.
   */
  public static class FloatColumn extends Column {
    private FloatArrayList arr;

    @Override
    public void initialize(int initialCapacity) {
      arr = new FloatArrayList(initialCapacity);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new FloatArrayList(byteBuffers.next().asFloatBuffer().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      ByteBuffer b = ByteBuffer.allocate(Float.SIZE / Byte.SIZE * arr.size());
      b.asFloatBuffer().put(arr.elements());
      return new ByteBuffer[] { b };
    }

    public float get(int index) {
      return arr.get(index);
    }

    public void add(float v) {
      arr.add(v);
    }
  }

  /**
   * Primitive double column.
   */
  public static class DoubleColumn extends Column {
    private DoubleArrayList arr;

    @Override
    public void initialize(int initialCapacity) {
      arr = new DoubleArrayList(initialCapacity);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new DoubleArrayList(byteBuffers.next().asDoubleBuffer().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      ByteBuffer b = ByteBuffer.allocate(Double.SIZE / Byte.SIZE * arr.size());
      b.asDoubleBuffer().put(arr.elements());
      return new ByteBuffer[] { b };
    }

    public double get(int index) {
      return arr.get(index);
    }

    public void add(double v) {
      arr.add(v);
    }
  }

  /**
   * Primitive byte column.
   */
  public static class ByteColumn extends Column {
    private ByteArrayList arr;

    @Override
    public void initialize(int initialCapacity) {
      arr = new ByteArrayList(initialCapacity);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new ByteArrayList(byteBuffers.next().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      return new ByteBuffer[] { ByteBuffer.allocate(arr.size()).put(
          arr.elements()) };
    }

    public byte get(int index) {
      return arr.get(index);
    }

    public void add(byte v) {
      arr.add(v);
    }
  }

  /**
   * Primitive boolean column: backed by a byte array, where each byte is one
   * boolean value.
   */
  public static class BooleanColumn extends Column {
    private ByteArrayList arr;

    @Override
    public void initialize(int initialCapacity) {
      arr = new ByteArrayList(initialCapacity);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new ByteArrayList(byteBuffers.next().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      return new ByteBuffer[] { ByteBuffer.allocate(arr.size()).put(
          arr.elements()) };
    }

    public boolean get(int index) {
      return arr.get(index) != 0;
    }

    public void add(boolean v) {
      arr.add((byte) (v ? 1 : 0));
    }
  }

  /**
   * Primitive boolean column: backed by a long array, where 32 boolean values
   * are packed into a single long.
   */
  public static class BooleanCompactColumn extends Column {

    private static final int WORD_LENS_IN_BITS = Long.SIZE;
    private LongArrayList arr;
    int size;

    @Override
    public void initialize(int initialCapacity) {
      arr = new LongArrayList(initialCapacity / WORD_LENS_IN_BITS);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new LongArrayList(byteBuffers.next().asLongBuffer().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      ByteBuffer b = ByteBuffer.allocate(Long.SIZE / Byte.SIZE * arr.size());
      b.asLongBuffer().put(arr.elements());
      return new ByteBuffer[] { b };
    }

    public boolean get(int index) {
      return (arr.get(index / WORD_LENS_IN_BITS) & (1L << index)) != 0;
    }

    public void add(boolean v) {
      if (size % WORD_LENS_IN_BITS == 0) {
        arr.add(v ? 1L : 0L);
      } else {
        long[] elements = arr.elements();
        elements[elements.length - 1] |= 1L << size;
      }
      size++;
    }
  }

  /**
   * String column: backed by a single byte array with a primitive integer array
   * for position indexing.
   */
  public static class StringColumn extends Column {

    private ByteArrayList arr;
    private IntArrayList posIndex;

    @Override
    public void initialize(int initialCapacity) {
      arr = new ByteArrayList(initialCapacity);
      posIndex = new IntArrayList(initialCapacity + 1);
      posIndex.add(0);
    }

    @Override
    public void initialize(Iterator<ByteBuffer> byteBuffers) {
      arr = new ByteArrayList(byteBuffers.next().array());
      posIndex = new IntArrayList(byteBuffers.next().asIntBuffer().array());
    }

    @Override
    public ByteBuffer[] asByteBuffers() {
      arr.trim();
      posIndex.trim();
      ByteBuffer b = ByteBuffer.allocate(arr.size()).put(arr.elements());
      ByteBuffer ib = ByteBuffer
          .allocate(Integer.SIZE / Byte.SIZE * arr.size());
      ib.asIntBuffer().put(posIndex.elements());
      return new ByteBuffer[] { b, ib };
    }

    public String get(int index) {
      int start = posIndex.getInt(index);
      return new String(arr.elements(), start, posIndex.getInt(index + 1)
          - start);
    }

    public void add(String s) {
      posIndex.add(arr.size() + s.length());
      byte[] bytes = s.getBytes();
      arr.addElements(arr.size(), bytes);
    }
  }

}
