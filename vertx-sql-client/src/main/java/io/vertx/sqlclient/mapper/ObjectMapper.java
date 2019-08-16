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
import io.vertx.sqlclient.mapper.ClassDissector.Dissection;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Object Mapper. 
 * Prepares mapping from column names to destination object class.
 * Applies previously mappings from Row objects to destination objects.
 * 
 * @author <a href="mailto:eduard.catala@gmail.com">Eduard Català</a>
 */
public class ObjectMapper {

  private static ObjectMapper instance;
  private final ClassDissector dissector;
  private final Map<Class, Map<String, Dissection>> dissectedClasses = new HashMap();

  public static synchronized ObjectMapper getInstance() {
    if (instance == null) {
      instance = new ObjectMapper();
    }
    return instance;
  }

  private ObjectMapper() {
    this.dissector = new ClassDissector();
  }

  public <T> List<MappedColumn> prepareMapping(List<String> columnNames, Class<T> clazz) {

    // Step 1: get class property method dissection
    Map<String, Dissection> classDissection = getDissection(clazz);

    // Step 2: map result columns to class properties
    int column = 0;
    List<MappedColumn> mappedColumns = new ArrayList<>(columnNames.size());
    for (String columnName : columnNames) {
      Dissection dis = classDissection.get(columnName.toLowerCase());
      if (dis != null && dis.getRowGetter() != null) {
        mappedColumns.add(new MappedColumn(column, dis));
      }
//      else {
//        System.out.println("Column " + columnName.toLowerCase() + " not mapped to any field");
//      }
      column++;
    }

    return mappedColumns;

  }

  private Map<String, Dissection> getDissection(Class clazz) {

    Map<String, Dissection> dissections = dissectedClasses.get(clazz);
    if (dissections == null) {
      dissections = dissector.dissect(clazz);
      dissectedClasses.put(clazz, dissections);
    }
    return dissections;
  }

  public <T> void map(Row row, T object, List<MappedColumn> mappedColumns, Handler<Throwable> exceptionHandler) {
    mappedColumns.forEach((MappedColumn mc) -> {
      Dissection d = mc.dissection;
      try {
        Object value = d.getRowGetter().invoke(row, mc.column);
        d.getSetter().invoke(object, value);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        } else {
          e.printStackTrace();
        }
      }
    });
  }

  public static final class MappedColumn {

    private int column;
    private Dissection dissection;

    public MappedColumn(int column, Dissection dissection) {
      this.column = column;
      this.dissection = dissection;
    }

    public int getColumn() {
      return column;
    }

    public Dissection getDissection() {
      return dissection;
    }

  }

}
