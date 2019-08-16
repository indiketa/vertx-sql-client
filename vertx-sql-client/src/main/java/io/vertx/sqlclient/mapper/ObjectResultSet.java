/*
 * Copyright 2019 Eclipse.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.sqlclient.mapper;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Generic result chain (RowSet with generic type).
 *
 * @author ecatala
 */
public class ObjectResultSet<T> implements Iterable<T> {

  private Chainable<T> head;
  private Chainable<T> tail;

  @Override
  public Iterator<T> iterator() {

    return new Iterator<T>() {

      Chainable<T> current = head;

      @Override
      public boolean hasNext() {
        return current != null;
      }

      @Override
      public T next() {
        if (current == null) {
          throw new NoSuchElementException();
        }
        Chainable<T> r = current;
        current = current.next;
        return r.value;
      }

    };

  }

  void chain(T value) {
    Chainable c = new Chainable(value);
    if (head == null) {
      head = tail = c;
    } else {
      tail.next = c;
      tail = c;
    }
  }

  private static class Chainable<T> {

    public Chainable(T value) {
      this.value = value;
    }

    T value;
    Chainable<T> next;
  }

}
