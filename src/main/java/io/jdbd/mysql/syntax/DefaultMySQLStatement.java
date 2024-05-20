/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jdbd.mysql.syntax;

import io.jdbd.mysql.protocol.SQLMode;
import io.jdbd.mysql.util.MySQLCollections;

import java.util.List;
import java.util.function.Predicate;

final class DefaultMySQLStatement implements MySQLStatement {

    private final String sql;

    private final List<String> staticSqlList;

    private final boolean backslashEscapes;

    private final boolean ansiQuotes;

    DefaultMySQLStatement(String sql, List<String> staticSqlList, boolean backslashEscapes, boolean ansiQuotes) {
        this.sql = sql;
        this.staticSqlList = MySQLCollections.unmodifiableList(staticSqlList);
        this.backslashEscapes = backslashEscapes;
        this.ansiQuotes = ansiQuotes;
    }

    @Override
    public List<String> sqlPartList() {
        return this.staticSqlList;
    }

    @Override
    public String originalSql() {
        return this.sql;
    }

    @Override
    public boolean isInvalid(Predicate<SQLMode> func) {
        return func.test(SQLMode.ANSI_QUOTES) != this.ansiQuotes
                || func.test(SQLMode.NO_BACKSLASH_ESCAPES) == this.backslashEscapes;
    }

}
