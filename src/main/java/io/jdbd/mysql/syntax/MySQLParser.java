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

import io.jdbd.JdbdException;
import io.jdbd.vendor.syntax.SQLParser;

public interface MySQLParser extends SQLParser {

    @Override
    MySQLStatement parse(String singleSql) throws JdbdException;

    boolean isSingleStmt(String sql) throws JdbdException;

    boolean isMultiStmt(String sql) throws JdbdException;

}
