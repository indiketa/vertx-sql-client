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

import io.vertx.core.Handler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.mapper.ObjectMapper.MappedColumn;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

/**
 * Provides collectors to map Rows
 *
 * @author <a href="mailto:eduard.catala@gmail.com">Eduard Català</a>
 */
public class SqlCollectors {

  public static <T> Collector<Row, ObjectResultSet<T>, ObjectResultSet<T>> toObject(Supplier<T> objectFactory) {
    return toObject(objectFactory, null);
  }

  public static <T> Collector<Row, ObjectResultSet<T>, ObjectResultSet<T>> toObject(Supplier<T> objectFactory, Handler<Throwable> exceptionHandler) {
    return new Collector<Row, ObjectResultSet<T>, ObjectResultSet<T>>() {

      private List<MappedColumn> mapping;
      private ObjectMapper mapper;

      private void initializeMapping(Row firstRow, Class<T> clazz) {
        mapper = ObjectMapper.getInstance();
        List<String> columnNames = new LinkedList<>();
        int col = 0;
        // Find another way to get the column names (expose rowdesc?)
        while (firstRow.getColumnName(col) != null) {
          columnNames.add(firstRow.getColumnName(col++));
        }
        mapping = ObjectMapper.getInstance().prepareMapping(columnNames, clazz);
      }

      @Override
      public Supplier<ObjectResultSet<T>> supplier() {
        return ObjectResultSet::new;
      }

      @Override
      public BiConsumer<ObjectResultSet<T>, Row> accumulator() {
        return (orr, row) -> {
          T instance = objectFactory.get();
          if (mapping == null) {
            initializeMapping(row, (Class<T>) instance.getClass());
          }
          mapper.map(row, instance, mapping, exceptionHandler);
          orr.chain(instance);
        };

      }

      @Override
      public BinaryOperator<ObjectResultSet<T>> combiner() {
        throw new UnsupportedOperationException("This should not happen");
      }

      @Override
      public Function<ObjectResultSet<T>, ObjectResultSet<T>> finisher() {
        return s -> s;
      }

      @Override
      public Set<Collector.Characteristics> characteristics() {
        return new HashSet();
      }

    };

  }

}
