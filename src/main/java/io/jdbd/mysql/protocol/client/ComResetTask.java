package io.jdbd.mysql.protocol.client;


import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;


/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_reset_connection.html">Protocol::COM_RESET_CONNECTION</a>
 */
final class ComResetTask extends MySQLTask {


    static Mono<Void> reset(TaskAdjutant adjutant) {
        return Mono.empty();
    }


    private ComResetTask(TaskAdjutant adjutant, Consumer<Throwable> errorConsumer) {
        super(adjutant, errorConsumer);
    }

    @Override
    protected Action onError(Throwable e) {
        return null;
    }

    @Override
    protected Publisher<ByteBuf> start() {
        return null;
    }

    @Override
    protected boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        return false;
    }
}
