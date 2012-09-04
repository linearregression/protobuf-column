package edu.berkeley.amplab.columnar;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public abstract class ColumnarCollection<E> implements Iterable<E> {

  /**
   * Create a new columnar collection.
   * 
   * @param initialCapacity
   */
  public abstract void initialize(int initialCapacity);

  /**
   * Create a columnar collection using the binary data from the list of
   * ByteBuffers. This is the counterpart of asByteBuffers().
   * 
   * @param byteBuffers
   *          The byte buffers to restore the collection from.
   */
  public abstract void initialize(Iterable<ByteBuffer> byteBuffers);

  /**
   * Serialize the collection into an array of ByteBuffer.
   * 
   * @return an array of ByteBuffer serializing this collection.
   */
  public abstract List<ByteBuffer> asByteBuffers();

  /**
   * Returns the number of elements in this collection.
   * 
   * @return the number of elements in this collection.
   */
  public abstract int size();

  /**
   * Returns the element at the specified position in this collection.
   * 
   * @param index
   *          index of the element to return
   * @return the element at the specified position in this list of type
   *         E.Columnar.
   */
  public abstract E get(int index);

  /**
   * Appends the specified element to the end of this collection.
   * 
   * @param elem
   *          element to be appended to this collection.
   */
  public abstract void add(E elem);

  /**
   * Return an iterator for the elements in this columnar collection. The
   * elements returned by the iterator should be of type E.Columnar.
   */
  public Iterator<E> iterator() {
    return new Itr();
  }

  /**
   * An iterator over the ColumnarCollection.
   */
  private class Itr implements java.util.Iterator<E> {

    int lastRet = -1; // index of last element returned; -1 if no such
    int cursor; // index of next element to return

    public boolean hasNext() {
      return cursor != size();
    }

    public E next() {
      int i = cursor;
      if (i >= size()) {
        throw new NoSuchElementException();
      }
      cursor = i + 1;
      lastRet = i;
      return get(lastRet);
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
