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
