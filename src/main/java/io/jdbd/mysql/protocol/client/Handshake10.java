package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLPacket;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.MySQLStrings;
import io.netty.buffer.ByteBuf;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
 */
final class Handshake10 implements MySQLPacket {

    /**
     * @param cumulateBuffer the cumulateBuffer that {@link ByteBuf#readerIndex()} is payload start index.
     */
    public static Handshake10 read(final ByteBuf cumulateBuffer, final int payloadLength) {
        final int writerIndex, limitIndex;
        writerIndex = cumulateBuffer.writerIndex();

        limitIndex = cumulateBuffer.readerIndex() + payloadLength;
        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(limitIndex);
        }

        final Handshake10 packet;
        packet = new Handshake10(cumulateBuffer);

        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(writerIndex);
        }
        return packet;
    }


    final MySQLServerVersion serverVersion;

    final long threadId;


    /**
     * server capability .
     */
    final int capabilityFlags;

    final short collationIndex;

    final int statusFlags;

    /**
     * optional
     */
    final short authPluginDataLen;


    final String pluginSeed;

    final String authPluginName;

    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
     */
    private Handshake10(final ByteBuf payload) {

        if (payload.readByte() != 10) { // protocol version
            throw new IllegalArgumentException("byteBuf isn't Handshake v10 .");
        }
        // 1. server version
        this.serverVersion = MySQLServerVersion.from(Packets.readStringTerm(payload, StandardCharsets.US_ASCII));
        // 2. thread id,a.k.a. connection id
        this.threadId = Packets.readInt4AsLong(payload);
        // 3. auth-plugin-data-part-1,first 8 bytes of the plugin provided data (scramble)
        final String authPluginDataPart1;
        authPluginDataPart1 = Packets.readStringFixed(payload, 8, StandardCharsets.US_ASCII);
        // 4. filler,0x00 byte, terminating the first part of a scramble
        payload.skipBytes(1);

        // 5. The lower 2 bytes of the Capabilities Flags
        int capabilityFlags = Packets.readInt2AsInt(payload);
        // 6. character_set,default server a_protocol_character_set, only the lower 8-bits
        this.collationIndex = (short) Packets.readInt1AsInt(payload);
        // 7. status_flags,SERVER_STATUS_flags_enum
        this.statusFlags = Packets.readInt2AsInt(payload);
        // 8. read the upper 2 bytes of the Capabilities Flags and OR operation
        capabilityFlags |= (Packets.readInt2AsInt(payload) << 16);

        this.capabilityFlags = capabilityFlags;
        // 9. auth_plugin_data_len or skip.
        final short authPluginDataLen;
        if ((capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            //length of the combined auth_plugin_data (scramble), if auth_plugin_data_len is > 0
            authPluginDataLen = (short) Packets.readInt1AsInt(payload);
        } else {
            //skip constant 0x00
            payload.readByte();
            authPluginDataLen = 0;
        }
        this.authPluginDataLen = authPluginDataLen;
        // 10. reserved,reserved. All 0s.   skip for update read index
        payload.skipBytes(10);

        // 11. auth-plugin-data-part-2,Rest of the plugin provided data (scramble), $len=MAX(13, length of auth-plugin-data - 8)
        final String authPluginDataPart2;
        authPluginDataPart2 = Packets.readStringFixed(payload, Integer.max(13, authPluginDataLen - 8),
                StandardCharsets.US_ASCII);
        // 12. auth_plugin_name,name of the auth_method that the auth_plugin_data belongs to
        String authPluginName = null;
        if ((capabilityFlags & Capabilities.CLIENT_PLUGIN_AUTH) != 0) {
            // Due to Bug#59453 the auth-plugin-name is missing the terminating NUL-char in versions prior to 5.5.10 and 5.6.2.
            if (!this.serverVersion.meetsMinimum(5, 5, 10)
                    || (this.serverVersion.meetsMinimum(5, 6, 0) && !this.serverVersion.meetsMinimum(5, 6, 2))) {
                authPluginName = Packets.readStringFixed(payload, authPluginDataLen, StandardCharsets.US_ASCII);
            } else {
                authPluginName = Packets.readStringTerm(payload, StandardCharsets.US_ASCII);
            }
        }

        this.pluginSeed = authPluginDataPart1 + authPluginDataPart2;
        this.authPluginName = authPluginName;
    }

    public MySQLServerVersion getServerVersion() {
        return this.serverVersion;
    }

    public long getThreadId() {
        return this.threadId;
    }


    public String getPluginSeed() {
        return pluginSeed;
    }


    public short getCollationIndex() {
        return this.collationIndex;
    }


    @Nullable
    public String getAuthPluginName() {
        return this.authPluginName;
    }

    @Override
    public String toString() {
        return MySQLStrings.builder()
                .append(getClass().getSimpleName())
                .append("[ serverVersion : ")
                .append(this.serverVersion)
                .append(" , threadId : ")
                .append(this.threadId)
                .append(" , capabilityFlags : ")
                .append(Integer.toBinaryString(this.capabilityFlags))
                .append(" , collationIndex : ")
                .append(this.collationIndex)
                .append(" , statusFlags : ")
                .append(Integer.toBinaryString(this.statusFlags))
                .append(" , pluginSeed : ")
                .append(this.pluginSeed)
                .append(" , authPluginName : ")
                .append(this.authPluginName)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}
