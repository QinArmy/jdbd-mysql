package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLCollections;
import io.netty.buffer.ByteBuf;
import io.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_ok_packet.html">Protocol::OK_Packet</a>
 */
final class OkPacket extends Terminator {

    static final int OK_HEADER = 0x00;


    static OkPacket readCumulate(final ByteBuf cumulateBuffer, final int payloadLength,
                                 final int capabilities) {
        final int writerIndex, limitIndex;
        writerIndex = cumulateBuffer.writerIndex();

        limitIndex = cumulateBuffer.readerIndex() + payloadLength;
        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(limitIndex);
        }


        final OkPacket packet;
        packet = read(cumulateBuffer, capabilities);

        if (limitIndex != writerIndex) {
            cumulateBuffer.writerIndex(writerIndex);
        }
        return packet;
    }

    /**
     * @param payload    a packet buffer than skip header .
     * @param capability <ul>
     *                   <li>before server receive handshake response packet: server capability</li>
     *                   <li>after server receive handshake response packet: negotiated capability</li>
     *                   </ul>
     * @throws IllegalArgumentException packet error.
     */
    static OkPacket read(final ByteBuf payload, final int capability) {
        final int type = Packets.readInt1AsInt(payload);
        if (type != OK_HEADER && type != EofPacket.EOF_HEADER) {
            throw new IllegalArgumentException("packetBuf isn't ok packet.");
        }
        //1. affected_rows
        final long affectedRows = Packets.readLenEnc(payload);
        //2. last_insert_id
        final long lastInsertId = Packets.readLenEnc(payload);
        //3. status_flags and warnings
        final int statusFags, warnings;
        if ((capability & Capabilities.CLIENT_PROTOCOL_41) != 0) {
            statusFags = Packets.readInt2AsInt(payload);
            warnings = Packets.readInt2AsInt(payload);
        } else if ((capability & Capabilities.CLIENT_TRANSACTIONS) != 0) {
            statusFags = Packets.readInt2AsInt(payload);
            warnings = 0;
        } else {
            statusFags = 0;
            warnings = 0;
        }
        //4.
        String info = null;
        StateOption stateOption = null;
        if ((capability & Capabilities.CLIENT_SESSION_TRACK) != 0) {
            if (payload.isReadable()) {
                // here , avoid ResultSet terminator.
                info = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
            }
            if (info == null) {
                info = "";
            }
            if ((statusFags & Terminator.SERVER_SESSION_STATE_CHANGED) != 0 && payload.isReadable()) {
                stateOption = readSessionStates(payload);
            }
        } else {
            info = Packets.readStringEof(payload, StandardCharsets.UTF_8);
        }
        return new OkPacket(affectedRows, lastInsertId
                , statusFags, warnings, info, stateOption);
    }

    static boolean isOkPacket(ByteBuf payloadBuf) {
        return Packets.getInt1AsInt(payloadBuf, payloadBuf.readerIndex()) == OK_HEADER;
    }

    private static final Logger LOG = LoggerFactory.getLogger(OkPacket.class);


    // SESSION_TRACK_SYSTEM_VARIABLES : Session system variables.
    private static final byte SESSION_TRACK_SYSTEM_VARIABLES = 0x00;
    // SESSION_TRACK_SCHEMA : Current schema.
    private static final byte SESSION_TRACK_SCHEMA = 0x01;
    // SESSION_TRACK_STATE_CHANGE : track session state changes
    private static final byte SESSION_TRACK_STATE_CHANGE = 0x02;
    // SESSION_TRACK_GTIDS : See also: session_track_gtids.
    private static final byte SESSION_TRACK_GTIDS = 0x03;
    // SESSION_TRACK_TRANSACTION_CHARACTERISTICS : Transaction chistics.
    private static final byte SESSION_TRACK_TRANSACTION_CHARACTERISTICS = 0x04;
    // SESSION_TRACK_TRANSACTION_STATE : Transaction state.
    private static final byte SESSION_TRACK_TRANSACTION_STATE = 0x05;


    final long affectedRows;

    final long lastInsertId;

    final String info;

    // below session state
    final StateOption stateOption;

    private OkPacket(long affectedRows, long lastInsertId, int statusFags, int warnings, String info,
                     @Nullable StateOption stateOption) {
        super(warnings, statusFags);

        this.affectedRows = affectedRows;
        this.lastInsertId = lastInsertId;
        this.info = info;
        this.stateOption = stateOption;
    }

    public long getAffectedRows() {
        return this.affectedRows;
    }

    public long getLastInsertId() {
        return this.lastInsertId;
    }

    public String getInfo() {
        return this.info;
    }

    @Nullable
    public StateOption getStateOption() {
        return this.stateOption;
    }


    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder("OkPacket{");
        builder.append("\naffectedRows=").append(this.affectedRows)
                .append("\n, lastInsertId=").append(this.lastInsertId)
                .append("\n, info='").append(this.info).append('\'')
                .append("\n, stateOption=").append(this.stateOption)
                .append("\n,warnings=").append(this.warnings);


        builder.append("\n,server status:\n");
        appendServerStatus(builder);
        return builder.append("\n}")
                .toString();
    }


    /**
     * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/mysql__com_8h.html#a1c6cf2629b0bda6da6788a25725d6b7f">enum_session_state_type</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/session-state-tracking.html">server documentation</a>
     */
    private static StateOption readSessionStates(final ByteBuf payload) {
        final int readerIndex = payload.readerIndex();
        final int end;
        end = readerIndex + Packets.readLenEncAsInt(payload);

        List<Pair<String, String>> variablePairList = null;
        String schema = null, txCharacteristics = null, txState = null, gtids = null, stateChange = null, unknown = null;

        for (int type, dataIndex, dataLength; payload.readerIndex() < end; ) {
            type = Packets.readInt1AsInt(payload);
            dataIndex = payload.readerIndex();
            dataLength = Packets.readLenEncAsInt(payload);
            switch (type) {
                case SESSION_TRACK_SYSTEM_VARIABLES: {
                    if (variablePairList == null) {
                        variablePairList = MySQLCollections.arrayList();
                    }
                    final int dataEnd = dataIndex + dataLength;
                    while (payload.readerIndex() < dataEnd) {
                        String name = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
                        String value = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
                        if (name != null && value != null) {
                            variablePairList.add(new Pair<>(name, value));
                        } else if (LOG.isDebugEnabled()) {
                            LOG.warn("MySQL server response SESSION_TRACK_SYSTEM_VARIABLES name:{} , value:{} error."
                                    , name, value);
                        }
                    }
                }
                break;
                case SESSION_TRACK_SCHEMA: {
                    schema = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
                    LOG.debug("SESSION_TRACK_SCHEMA schema:{}", schema);
                }
                break;
                case SESSION_TRACK_TRANSACTION_CHARACTERISTICS: {//single value, transaction characteristics statement
                    txCharacteristics = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
                    LOG.debug("SESSION_TRACK_TRANSACTION_CHARACTERISTICS :{}", txCharacteristics);
                }
                break;
                case SESSION_TRACK_TRANSACTION_STATE: {//single value, transaction state record
                    txState = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
                    //T_______
                    //T_R_____
                    LOG.debug("SESSION_TRACK_TRANSACTION_STATE transactionState:{}", txState);
                }
                break;
                case SESSION_TRACK_GTIDS: {//single value, list of GTIDs as reported by server
                    payload.readByte(); // skip the byte reserved for the encoding specification, see WL#6128
                    gtids = Packets.readStringLenEnc(payload, StandardCharsets.UTF_8);
                    LOG.debug("SESSION_TRACK_GTIDS:{}", gtids);
                }
                break;
                case SESSION_TRACK_STATE_CHANGE: {//single value, "1" or "0";
                    stateChange = Packets.readStringFixed(payload, dataLength, StandardCharsets.UTF_8);
                    LOG.debug("SESSION_TRACK_STATE_CHANGE :{}", stateChange);
                }
                break;
                default: {
                    // new type.
                    unknown = Packets.readStringFixed(payload, dataLength, StandardCharsets.UTF_8);
                    LOG.debug("Unknown session_state_type :{}", unknown);
                }

            }
        }


        if (payload.readerIndex() < end) {
            LOG.debug("no read complete.rest {} bytes.", end - payload.readerIndex());
        }

        payload.readerIndex(end); // avoid tail filler.
        return new StateOption(variablePairList, schema, txCharacteristics, txState, gtids, stateChange, unknown);

    }

    private static final class StateOption {

        private final List<Pair<String, String>> variablePairList;

        private final String schema;

        private final String txCharacteristics;

        private final String txState;

        private final String gtids;

        private final String stateChange;

        private final String unknown;

        private StateOption(@Nullable List<Pair<String, String>> variablePairList, @Nullable String schema,
                            @Nullable String txCharacteristics, @Nullable String txState,
                            @Nullable String gtids, @Nullable String stateChange, @Nullable String unknown) {
            this.variablePairList = variablePairList == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(variablePairList);
            this.schema = schema;
            this.txCharacteristics = txCharacteristics;
            this.txState = txState;
            this.gtids = gtids;
            this.stateChange = stateChange;
            this.unknown = unknown;
        }

        @Override
        public String toString() {
            final StringBuilder builder = new StringBuilder("{");
            final List<Pair<String, String>> pairList = this.variablePairList;
            if (pairList.size() > 0) {
                builder.append("\nvariables={");
                for (Pair<String, String> pair : pairList) {
                    builder.append("\nname:")
                            .append(pair.getFirst())
                            .append(",value:")
                            .append(pair.getSecond());

                }
                builder.append('}');
            }

            if (this.schema != null) {
                builder.append("\n, schema=")
                        .append(this.schema);
            }
            if (this.txCharacteristics != null) {
                builder.append("\n, txCharacteristics=")
                        .append(this.txCharacteristics);
            }
            if (this.txState != null) {
                builder.append("\n, txState=")
                        .append(this.txState);
            }

            if (this.gtids != null) {
                builder.append("\n, gtids=")
                        .append(this.gtids);
            }

            if (this.stateChange != null) {
                builder.append("\n, stateChange=")
                        .append(this.stateChange);
            }

            if (this.unknown != null) {
                builder.append("\n, unknown=")
                        .append(this.unknown);
            }
            return builder.append("\n}")
                    .toString();
        }


    }//StateOption


}
