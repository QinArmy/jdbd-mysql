package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.StringJoiner;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_err_packet.html">Protocol::ERR_Packet</a>
 */
public final class ErrorPacket implements MySQLPacket {

    public static final short ERROR_HEADER = 0xFF;

    static ErrorPacket readCumulate(final ByteBuf cumulateBuffer, final int payloadLength,
                                    final int capabilities, final Charset errorMsgCharset) {
        final int writerIndex, limitIndex;
        writerIndex = cumulateBuffer.writerIndex();

        limitIndex = cumulateBuffer.readerIndex() + payloadLength;
        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(limitIndex);
        }


        final ErrorPacket packet;
        packet = read(cumulateBuffer, capabilities, errorMsgCharset);

        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(writerIndex);
        }
        return packet;
    }


    public static ErrorPacket read(final ByteBuf payload, final int capability, final Charset errorMsgCharset) {
        if (Packets.readInt1AsInt(payload) != ERROR_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        }
        final int errorCode;
        errorCode = Packets.readInt2AsInt(payload);

        String sqlStateMarker = null, sqlState = null, errorMessage;
        if ((capability & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            sqlStateMarker = Packets.readStringFixed(payload, 1, errorMsgCharset);
            sqlState = Packets.readStringFixed(payload, 5, errorMsgCharset);
        }
        errorMessage = Packets.readStringEof(payload, payload.readableBytes(), errorMsgCharset);

        return new ErrorPacket(errorCode, sqlStateMarker
                , sqlState, errorMessage
        );
    }


    private final int errorCode;

    private final String sqlStateMarker;

    private final String sqlState;

    private final String errorMessage;

    private ErrorPacket(int errorCode, @Nullable String sqlStateMarker, @Nullable String sqlState, String errorMessage) {
        this.errorCode = errorCode;
        this.sqlStateMarker = sqlStateMarker;
        this.sqlState = sqlState;
        this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
        return this.errorCode;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }

    @Nullable
    public String getSqlStateMarker() {
        return this.sqlStateMarker;
    }

    @Nullable
    public String getSqlState() {
        return this.sqlState;
    }


    @Override
    public String toString() {
        return new StringJoiner(", ", ErrorPacket.class.getSimpleName() + "[", "]")
                .add("errorCode=" + errorCode)
                .add("sqlStateMarker='" + sqlStateMarker + "'")
                .add("sqlState='" + sqlState + "'")
                .add("errorMessage='" + errorMessage + "'")
                .toString();
    }

    public static boolean isErrorPacket(ByteBuf byteBuf) {
        return Packets.getInt1AsInt(byteBuf, byteBuf.readerIndex()) == ERROR_HEADER;
    }

}



