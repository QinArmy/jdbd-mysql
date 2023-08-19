package io.jdbd.mysql.protocol.client;

import io.jdbd.vendor.env.Properties;
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

    final Properties properties;

    MySQLTask(TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
        this.adjutant = adjutant;
        this.properties = adjutant.host().getProperties();
    }


    @Override
    protected boolean skipPacketsOnError(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        System.out.println("skipPacketsOnError");
        return true;
    }


}
