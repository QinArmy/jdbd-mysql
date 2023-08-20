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
