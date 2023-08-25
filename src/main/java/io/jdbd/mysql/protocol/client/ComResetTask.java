package io.jdbd.mysql.protocol.client;


import io.jdbd.JdbdException;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.session.SessionCloseException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;


/**
 * <p>
 * This class send reset packet.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_reset_connection.html">Protocol::COM_RESET_CONNECTION</a>
 */
final class ComResetTask extends MySQLTask {


    static Mono<Void> reset(final TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                ComResetTask task = new ComResetTask(adjutant, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }
        });
    }

    private final MonoSink<Void> sink;

    private boolean taskEnd;

    private ComResetTask(TaskAdjutant adjutant, MonoSink<Void> sink) {
        super(adjutant, sink::error);
        this.sink = sink;
    }

    @Override
    protected Publisher<ByteBuf> start() {
        if (this.taskEnd) {
            return null;
        }
        final ByteBuf packet;
        packet = this.adjutant.allocator().buffer(5);
        Packets.writeInt3(packet, 1);
        packet.writeByte(0);
        packet.writeByte(0x1F); // COM_RESET_CONNECTION
        return Mono.just(packet);
    }

    @Override
    protected boolean decode(final ByteBuf cumulateBuffer, final Consumer<Object> serverStatusConsumer) {
        final boolean taskEnd;
        if (this.taskEnd) {
            taskEnd = true;
        } else if (!Packets.hasOnePacket(cumulateBuffer)) {
            taskEnd = false;
        } else {
            final int payloadLength;
            payloadLength = Packets.readInt3(cumulateBuffer);
            cumulateBuffer.readByte(); // skip sequenceId
            switch (Packets.getInt1AsInt(cumulateBuffer, cumulateBuffer.readerIndex())) {
                case OkPacket.OK_HEADER: {
                    final OkPacket ok;
                    ok = OkPacket.readCumulate(cumulateBuffer, payloadLength, this.adjutant.capability());
                    serverStatusConsumer.accept(ok);
                    taskEnd = true;
                }
                break;
                case MySQLServerException.ERROR_HEADER: { // no bug,never here
                    MySQLServerException error;
                    error = MySQLServerException.read(cumulateBuffer, payloadLength, this.adjutant.capability(),
                            this.adjutant.errorCharset());
                    addError(error);
                    taskEnd = true;
                }
                break;
                default:
                    throw new JdbdException("server return error response.");
            }
        }
        this.taskEnd = taskEnd;

        if (taskEnd) {
            if (hasError()) {
                publishError(this.sink::error);
            } else {
                this.sink.success();
            }
        }
        return taskEnd;
    }

    @Override
    protected Action onError(Throwable e) {
        if (!this.taskEnd) {
            this.taskEnd = true;
            this.sink.error(MySQLExceptions.wrap(e));
        }
        return Action.TASK_END;
    }


    @Override
    protected void onChannelClose() {
        if (!this.taskEnd) {
            this.taskEnd = true;
            this.sink.error(new SessionCloseException("unexpected session close"));
        }
    }


}
