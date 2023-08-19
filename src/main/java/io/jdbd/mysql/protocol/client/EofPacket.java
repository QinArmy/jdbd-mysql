package io.jdbd.mysql.protocol.client;

import io.netty.buffer.ByteBuf;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html">Protocol::EOF_Packet</a>
 */
public final class EofPacket extends Terminator {

    public static final short EOF_HEADER = 0xFE;

    public static EofPacket readCumulate(final ByteBuf cumulateBuffer, final int payloadLength,
                                         final int capabilities) {
        final int writerIndex, limitIndex;
        writerIndex = cumulateBuffer.writerIndex();

        limitIndex = cumulateBuffer.readerIndex() + payloadLength;
        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(limitIndex);
        }


        final EofPacket packet;
        packet = read(cumulateBuffer, capabilities);

        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(writerIndex);
        }
        return packet;
    }

    public static EofPacket read(final ByteBuf payload, final int capabilities) {
        if (Packets.readInt1AsInt(payload) != EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        } else if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) == 0) {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        final int statusFags, warnings;
        statusFags = Packets.readInt2AsInt(payload);
        warnings = Packets.readInt2AsInt(payload);
        return new EofPacket(statusFags, warnings);
    }


    private EofPacket(int statusFags, int warnings) {
        super(warnings, statusFags);
    }


}
