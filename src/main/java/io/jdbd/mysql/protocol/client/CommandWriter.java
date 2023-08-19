package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;

/**
 * <p>
 * This interface representing COM_STMT_EXECUTE writer.
 * </p>
 *
 * @since 1.0
 */
interface CommandWriter {


    Publisher<ByteBuf> writeCommand(int batchIndex) throws JdbdException;


}
