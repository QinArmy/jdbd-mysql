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

import io.jdbd.result.ResultItem;

import java.nio.file.Path;

interface StmtTask extends MetaAdjutant {

    boolean isCancelled();

    boolean hasError();

    void next(ResultItem result);

    void addErrorToTask(Throwable error);

    void addBigColumnPath(Path path);

    TaskAdjutant adjutant();

    void updateSequenceId(int sequenceId);


    int nextResultNo();


    boolean isBatchStmt();


    boolean hasMoreBatchGroup();

    int batchSize();

    int batchNo();


}
