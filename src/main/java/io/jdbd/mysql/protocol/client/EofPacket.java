/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        cumulateBuffer.readerIndex(limitIndex); // avoid tailor filler
        return packet;
    }

    public static EofPacket read(final ByteBuf payload, final int capabilities) {
        if (Packets.readInt1AsInt(payload) != EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't error packet.");
        } else if ((capabilities & Capabilities.CLIENT_PROTOCOL_41) == 0) {
            throw new IllegalArgumentException("only supported CLIENT_PROTOCOL_41.");
        }
        final int statusFags, warnings;
        warnings = Packets.readInt2AsInt(payload);
        statusFags = Packets.readInt2AsInt(payload);
        return new EofPacket(warnings, statusFags);
    }


    private EofPacket(int warnings, int statusFags) {
        super(warnings, statusFags);
    }


}
