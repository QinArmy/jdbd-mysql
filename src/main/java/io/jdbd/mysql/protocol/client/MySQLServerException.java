package io.jdbd.mysql.protocol.client;

import io.jdbd.lang.Nullable;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.result.ServerException;
import io.jdbd.session.Option;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;


/**
 * <p>
 * This class representing server error message.
 * </p>
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

        return new MySQLServerException(errorMessage, sqlState, errorCode, sqlStateMarker);
    }


    private static final Option<String> SQL_STATE_MARKER = Option.from("SQL_STATE_MARKER", String.class);

    private final String sqlStateMarker;

    private MySQLServerException(String message, @Nullable String sqlState, int vendorCode,
                                 @Nullable String sqlStateMarker) {
        super(message, sqlState, vendorCode);
        this.sqlStateMarker = sqlStateMarker;
    }


    @SuppressWarnings("unchecked")
    @Override
    public <T> T valueOf(final Option<T> option) {
        final Object value;
        if (SQL_STATE_MARKER.equals(option)) {
            value = this.sqlStateMarker;
        } else if (option == Option.MESSAGE) {
            value = getMessage();
        } else if (option == Option.SQL_STATE) {
            value = getSqlState();
        } else if (option == Option.VENDOR_CODE) {
            value = getVendorCode();
        } else {
            value = null;
        }
        return (T) value;
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getName())
                .append("[ message : ")
                .append(getMessage())
                .append(" , sqlState : ")
                .append(getSqlState())
                .append(" , vendorCode : ")
                .append(getVendorCode())
                .append(" , sqlStateMarker : ")
                .append(this.sqlStateMarker)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
