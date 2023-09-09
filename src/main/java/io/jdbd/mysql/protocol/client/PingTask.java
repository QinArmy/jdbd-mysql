package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.session.SessionCloseException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

/**
 * Ping task
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_ping.html">Protocol::COM_PING</a>
 * @since 1.0
 */
final class PingTask extends MySQLTask {

    static Mono<Void> ping(final TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                PingTask task = new PingTask(sink, adjutant);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrapIfNonJvmFatal(e));
            }
        });
    }


    private final MonoSink<Void> sink;


    private boolean taskEnd;


    private PingTask(MonoSink<Void> sink, TaskAdjutant adjutant) {
        super(adjutant, sink::error);
        this.sink = sink;
    }

    @Override
    protected Publisher<ByteBuf> start() {
        final ByteBuf packet = this.adjutant.allocator().buffer(5);
        Packets.writeInt3(packet, 1);
        packet.writeByte(0);
        packet.writeByte(0x0E); // COM_PING
        return Mono.just(packet);
    }

    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        if (!Packets.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        final int payloadLength;
        payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.readByte(); // skip sequenceId
        final OkPacket ok;
        ok = OkPacket.readCumulate(cumulateBuffer, payloadLength, this.adjutant.capability());
        serverStatusConsumer.accept(ok);
        this.taskEnd = true;
        this.sink.success();
        return true;
    }

    @Override
    protected void onChannelClose() {
        if (this.taskEnd) {
            return;
        }
        this.taskEnd = true;
        this.addError(new SessionCloseException("Session unexpected closed."));
        publishError(this.sink::error);
    }

    @Override
    protected Action onError(final Throwable e) {
        if (!this.taskEnd) {
            addError(e);
            publishError(this.sink::error);
        }
        return Action.TASK_END;
    }


}
