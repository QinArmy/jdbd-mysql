package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.result.ServerException;
import io.jdbd.session.Option;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.util.function.Function;


/**
 * <p>
 * This class representing server error message.
 * <br/>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html">Protocol::ERR_Packet</a>
 * @since 1.0
 */
final class MySQLServerException extends ServerException {

    static final int ERROR_HEADER = 0xFF;

    static boolean isErrorPacket(ByteBuf byteBuf) {
        return Packets.getInt1AsInt(byteBuf, byteBuf.readerIndex()) == ERROR_HEADER;
    }

    static MySQLServerException read(final ByteBuf cumulateBuffer, final int payloadLength,
                                     final int capabilities, final Charset errorMsgCharset) {
        final int writerIndex, limitIndex;
        writerIndex = cumulateBuffer.writerIndex();

        limitIndex = cumulateBuffer.readerIndex() + payloadLength;
        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(limitIndex);
        }

        // read start
        if (Packets.readInt1AsInt(cumulateBuffer) != ERROR_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        final int errorCode;
        errorCode = Packets.readInt2AsInt(cumulateBuffer);

        String sqlStateMarker = null, sqlState = null, errorMessage;
        if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            sqlStateMarker = Packets.readStringFixed(cumulateBuffer, 1, errorMsgCharset);
            sqlState = Packets.readStringFixed(cumulateBuffer, 5, errorMsgCharset);
        }
        errorMessage = Packets.readStringEof(cumulateBuffer, cumulateBuffer.readableBytes(), errorMsgCharset);

        // read end
        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(writerIndex);
        }
        cumulateBuffer.readerIndex(limitIndex); // avoid tailor filler


        final Function<Option<?>, ?> optionFunc;
        if (sqlStateMarker == null) {
            optionFunc = Option.EMPTY_OPTION_FUNC;
        } else {
            final String finalSqlStateMarker = sqlStateMarker;
            optionFunc = option -> {
                if (SQL_STATE_MARKER.equals(option)) {
                    return finalSqlStateMarker;
                }
                return null;
            };
        }
        return new MySQLServerException(errorMessage, sqlState, errorCode, optionFunc);
    }


    private static final Option<String> SQL_STATE_MARKER = Option.from("SQL_STATE_MARKER", String.class);

    private MySQLServerException(String message, @Nullable String sqlState, int vendorCode,
                                 Function<Option<?>, ?> optionFunc) {
        super(message, sqlState, vendorCode, optionFunc);
    }


}
