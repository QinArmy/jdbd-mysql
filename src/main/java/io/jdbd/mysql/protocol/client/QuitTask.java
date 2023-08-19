package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLExceptions;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.util.function.Consumer;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_quit.html">Protocol::COM_QUIT</a>
 */
final class QuitTask extends MySQLTask {

    static <T> Mono<T> quit(TaskAdjutant adjutant) {
        return Mono.create(sink -> {
            try {
                QuitTask task = new QuitTask(adjutant, sink);
                task.submit(sink::error);
            } catch (Throwable e) {
                sink.error(MySQLExceptions.wrap(e));
            }

        });
    }

    private static final Logger LOG = LoggerFactory.getLogger(QuitTask.class);

    private final MonoSink<?> sink;

    private boolean taskEnd;

    private QuitTask(TaskAdjutant adjutant, MonoSink<?> sink) {
        super(adjutant, sink::error);
        this.sink = sink;
    }


    @Override
    protected Publisher<ByteBuf> start() {
        ByteBuf packetBuf = Packets.createOnePacket(this.adjutant.allocator(), 1);
        packetBuf.writeByte(Packets.COM_QUIT_HEADER);
        Packets.writeHeader(packetBuf, 0);
        return Mono.just(packetBuf);
    }

    @Override
    protected boolean decode(ByteBuf cumulateBuffer, Consumer<Object> serverStatusConsumer) {
        if (!Packets.hasOnePacket(cumulateBuffer)) {
            return false;
        }
        final int payloadLength = Packets.readInt3(cumulateBuffer);
        cumulateBuffer.readByte();
        final int payloadStartIndex = cumulateBuffer.readerIndex();

        ErrorPacket error;
        error = ErrorPacket.read(cumulateBuffer, this.adjutant.capability(), this.adjutant.errorCharset());
        cumulateBuffer.readerIndex(payloadStartIndex + payloadLength);

        this.sink.error(MySQLExceptions.createErrorPacketException(error));
        this.taskEnd = true;
        return true;
    }

    @Override
    protected Action onError(Throwable e) {
        if (this.taskEnd) {
            LOG.error("Unknown error.", e);
        } else {
            this.sink.error(MySQLExceptions.wrap(e));
        }
        return Action.TASK_END;
    }

    @Override
    public void onChannelClose() {
        this.taskEnd = true;
        this.sink.success();
    }


}
