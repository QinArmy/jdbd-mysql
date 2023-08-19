package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.env.MySQLUrlParser;
import io.jdbd.mysql.protocol.MySQLProtocol;
import io.jdbd.mysql.util.MySQLCollections;
import org.testng.Assert;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

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
        return ClientProtocolFactory.from(MySQLUrlParser.parse((String) map.get("url"), map).get(0));
    }


    /*################################## blow private method ##################################*/

    private static ClientProtocolFactory createProtocolFactory() {
        final Map<String, Object> map;
        map = ClientTestUtils.loadConfigMap();
        map.put("sslMode", Enums.SslMode.DISABLED.name());
        return ClientProtocolFactory.from(MySQLUrlParser.parse((String) map.get("url"), map).get(0));
    }





}
