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

package io.jdbd.mysql;

public interface Groups {


    String MYSQL_URL = "mysqlUrl";

    String AUTHENTICATE_PLUGIN = "authenticatePlugin";

    String SQL_PARSER = "sqlParser";

    String SESSION_INITIALIZER = "sessionInitializer";

    String DATA_PREPARE = "dataPrepare";

    String COM_QUERY = "comQuery";

    String LOAD_DATA = "loadData";

    String COM_QUERY_WRITER = "comQueryWriter";

    String COM_STMT_PREPARE = "comStmtPrepare";

    String TEXT_RESULT_BIG_COLUMN = "TextResultSetReader";

    String MULTI_STMT = "multiStatement";

    String UTILS = "utils";

}
