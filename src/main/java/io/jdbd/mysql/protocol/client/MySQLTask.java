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

import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.task.CommunicationTask;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

/**
 * Base class of All MySQL task.
 *
 * @see ComQueryTask
 * @see ComPreparedTask
 * @see QuitTask
 */
abstract class MySQLTask extends CommunicationTask {


    final TaskAdjutant adjutant;

    final Environment env;

    MySQLTask(TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
        this.adjutant = adjutant;
        this.env = adjutant.host().properties();
    }


    @Override
    protected boolean skipPacketsOnError(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        System.out.println("skipPacketsOnError");
        // TODO
        return true;
    }


}
