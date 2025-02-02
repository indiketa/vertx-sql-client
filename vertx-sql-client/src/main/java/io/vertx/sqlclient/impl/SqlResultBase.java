/*
 * Copyright (C) 2017 Julien Viet
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.vertx.sqlclient.impl;

import io.vertx.sqlclient.PropertyKind;
import io.vertx.sqlclient.SqlResult;

import java.util.List;
import java.util.Map;

public abstract class SqlResultBase<T, R extends SqlResultBase<T, R>> implements SqlResult<T> {

  int updated;
  List<String> columnNames;
  int size;
  R next;
  Map<PropertyKind<?>, Object> properties;

  @Override
  public List<String> columnsNames() {
    return columnNames;
  }

  @Override
  public int rowCount() {
    return updated;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public <V> V property(PropertyKind<V> property) {
    if (property == null) {
      throw new IllegalArgumentException("Property can not be null");
    }
    if (properties == null) {
      return null;
    } else {
      Object value = properties.get(property);
      Class<V> type = property.type();
      // if the property is unknown or the value is null then we return null to the user
      return type.cast(value);
    }
  }

  @Override
  public R next() {
    return next;
  }
}
