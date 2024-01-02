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

package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.Groups;
import io.jdbd.result.CurrentRow;
import io.jdbd.result.ResultRow;
import io.jdbd.result.ResultStates;
import io.jdbd.vendor.stmt.ParamStmt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @see QueryCommandWriter
 */
@Deprecated
@Test(enabled = false, groups = {Groups.COM_QUERY_WRITER}, dependsOnGroups = {Groups.SESSION_INITIALIZER, Groups.UTILS})
public class QueryCommandWriterSuiteTests extends AbstractStmtTaskSuiteTests {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCommandWriterSuiteTests.class);

    public QueryCommandWriterSuiteTests() {
        super(SubType.COM_QUERY);
    }

    @Override
    Mono<ResultStates> executeUpdate(ParamStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.paramUpdate(stmt, adjutant);
    }

    @Override
    Flux<ResultRow> executeQuery(ParamStmt stmt, TaskAdjutant adjutant) {
        return ComQueryTask.paramQuery(stmt, CurrentRow.AS_RESULT_ROW, ResultStates.IGNORE_STATES, adjutant);
    }

    @Override
    Logger obtainLogger() {
        return LOG;
    }


    @Test(timeOut = TIME_OUT)
    public void createStaticSingleCommand() {
        LOG.info("createStaticSingleCommand test start");
        //final MySQLTaskAdjutant adjutant = obtainTaskAdjutant();
        //TODO zoro add test code
        LOG.info("createStaticSingleCommand test success");
        // releaseConnection(adjutant);
    }


}
