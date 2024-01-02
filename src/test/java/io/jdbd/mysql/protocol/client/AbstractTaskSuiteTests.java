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

import io.jdbd.mysql.ClientTestUtils;
import io.jdbd.mysql.env.MySQLUrlParser;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import org.testng.Assert;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

@Deprecated
public abstract class AbstractTaskSuiteTests {

    static final long TIME_OUT = 5 * 1000L;

    static final Queue<TaskAdjutant> TASK_ADJUTANT_QUEUE = new LinkedBlockingQueue<>();

    static final ClientProtocolFactory PROTOCOL_FACTORY = createProtocolFactory();


    protected static TaskAdjutant obtainTaskAdjutant() {
        TaskAdjutant adjutant = TASK_ADJUTANT_QUEUE.poll();
        if (adjutant == null) {
            adjutant = PROTOCOL_FACTORY.createProtocol()
                    .map(AbstractTaskSuiteTests::getTaskAdjutant)
                    .block();
            Assert.assertNotNull(adjutant, "adjutant");
        }
        return adjutant;
    }


    protected static void releaseConnection(TaskAdjutant adjutant) {
        TASK_ADJUTANT_QUEUE.offer(adjutant);
    }


    static TaskAdjutant getTaskAdjutant(MySQLProtocol protocol) {
        return ((ClientProtocol) protocol).adjutant;
    }

    static ClientProtocolFactory createProtocolFactory(final Map<String, Object> props) {
        final Map<String, Object> map;
        map = MySQLCollections.hashMap(ClientTestUtils.loadConfigMap());

        map.putAll(props);
        return ClientProtocolFactory.from(MySQLUrlParser.parse((String) map.get("url"), map).get(0), false);
    }


    /*################################## blow private method ##################################*/

    private static ClientProtocolFactory createProtocolFactory() {
        final Map<String, Object> map;
        map = ClientTestUtils.loadConfigMap();
        map.put("sslMode", Enums.SslMode.DISABLED.name());
        return ClientProtocolFactory.from(MySQLUrlParser.parse((String) map.get("url"), map).get(0), false);
    }


}
