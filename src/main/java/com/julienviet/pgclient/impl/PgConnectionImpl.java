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

package com.julienviet.pgclient.impl;

import com.julienviet.pgclient.PgConnection;
import com.julienviet.pgclient.PgPreparedStatement;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.UpdateResult;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author <a href="mailto:julien@julienviet.com">Julien Viet</a>
 */
class PgConnectionImpl implements PgConnection {

  private DbConnection dbConnection;
  private final Map<String, PgPreparedStatement> psCache;

  public PgConnectionImpl(DbConnection dbConnection, boolean cachePreparedStatements) {
    this.dbConnection = dbConnection;
    this.psCache = cachePreparedStatements ? new ConcurrentHashMap<>() : null;
  }

  @Override
  public PgConnection execute(String sql, Handler<AsyncResult<ResultSet>> handler) {
    dbConnection.schedule(new QueryCommand(sql, new ResultSetBuilder(handler)));
    return this;
  }

  @Override
  public PgConnection update(String sql, Handler<AsyncResult<UpdateResult>> handler) {
    dbConnection.schedule(new UpdateCommand(sql, handler));
    return this;
  }

  @Override
  public PgConnection query(String sql, Handler<AsyncResult<ResultSet>> handler) {
    dbConnection.schedule(new QueryCommand(sql, new ResultSetBuilder(handler)));
    return this;
  }

  @Override
  public PgConnection closeHandler(Handler<Void> handler) {
    dbConnection.closeHandler(handler);
    return this;
  }

  @Override
  public PgConnection exceptionHandler(Handler<Throwable> handler) {
    dbConnection.exceptionHandler(handler);
    return this;
  }

  @Override
  public void close() {
    dbConnection.doClose();
  }

  @Override
  public PgPreparedStatement prepare(String sql) {
    if (psCache != null) {
      return psCache.computeIfAbsent(sql, this::createCachedPreparedStatement);
    } else {
      return new PreparedStatementImpl(dbConnection, sql, UUID.randomUUID().toString(), false);
    }
  }

  private PreparedStatementImpl createCachedPreparedStatement(String sql) {
    return new PreparedStatementImpl(dbConnection, sql, UUID.randomUUID().toString(), true);
  }
}
