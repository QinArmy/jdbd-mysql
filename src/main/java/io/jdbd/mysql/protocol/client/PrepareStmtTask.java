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

import io.jdbd.vendor.stmt.ParamSingleStmt;
import io.netty.buffer.ByteBuf;
import reactor.core.publisher.Mono;

interface PrepareStmtTask {

    ParamSingleStmt getStmt();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    int getStatementId();

    /**
     * @throws IllegalStateException throw when before prepare.
     */
    MySQLColumnMeta[] getParameterMetas();

    TaskAdjutant adjutant();

    int nextSequenceId();

   void resetSequenceId();

    void addErrorToTask(Throwable error);

    boolean isSupportFetch();

    void nextGroupReset();

    Mono<ByteBuf> handleExecuteMessageError(Throwable error);


}
