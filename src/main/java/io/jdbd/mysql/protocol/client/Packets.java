package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.vendor.stmt.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

abstract class Packets {


    private Packets() {
        throw new UnsupportedOperationException();
    }

    static final long NULL_LENGTH = -1L;

    static final byte HEADER_SIZE = 4;


    static final int MAX_PAYLOAD = 0xFFFF_FF;

    static final int MAX_PACKET = HEADER_SIZE + MAX_PAYLOAD;

    static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * @see #ENC_3
     */
    static final int ENC_3_MAX_VALUE = 0xFF_FF_FF;

    static final short LOCAL_INFILE = 0xFB;
    static final byte COM_QUERY = 0x03;
    static final byte COM_STMT_PREPARE = 0x16;
    static final byte COM_STMT_EXECUTE = 0x17;

    static final byte COM_STMT_SEND_LONG_DATA = 0x18;
    static final byte COM_STMT_CLOSE = 0x19;
    static final byte COM_STMT_FETCH = 0x1C;

    static final byte COM_RESET_CONNECTION = 0x1F;
    static final byte COM_SET_OPTION = 0x1B;

    static final byte COM_STMT_RESET = 0x1A;
    static final byte COM_QUIT_HEADER = 0x01;


    static final short ENC_0 = 0xFB;
    static final short ENC_3 = 0xFC;
    static final short ENC_4 = 0xFD;
    static final short ENC_9 = 0xFE;

    static final int BIT_8 = 0xFF;

    static final long BIT_8L = 0xFFL;

    static final long BIT_32 = 0xFFFF_FFFFL;

    private static final long LONG_SIGNED_BIT = (1L << 63);


    public static int readInt1AsInt(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8);
    }


    public static int getInt1AsInt(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index) & BIT_8);
    }


    public static int readInt2AsInt(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8) | ((byteBuf.readByte() & BIT_8) << 8);
    }


    public static int getInt2AsInt(ByteBuf byteBuf, int index) {
        return byteBuf.getUnsignedShortLE(index);
    }


    public static int readInt3(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8)
                | ((byteBuf.readByte() & BIT_8) << 8)
                | ((byteBuf.readByte() & BIT_8) << 16);
    }

    public static int getInt3(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8)
                | ((byteBuf.getByte(index++) & BIT_8) << 8)
                | ((byteBuf.getByte(index) & BIT_8) << 16);
    }

    public static long readInt4AsLong(ByteBuf byteBuf) {
        return readInt4(byteBuf) & 0xFFFF_FFFFL;
    }

    public static int readInt4(ByteBuf byteBuf) {
        return byteBuf.readIntLE();
    }

    public static long getInt4AsLong(ByteBuf byteBuf, int index) {
        return byteBuf.getUnsignedIntLE(index);
    }


    public static int getInt4(ByteBuf byteBuf, int index) {
        return byteBuf.getIntLE(index);
    }


    public static long readInt6(ByteBuf byteBuf) {
        return (byteBuf.readByte() & BIT_8L)
                | ((byteBuf.readByte() & BIT_8L) << 8)
                | ((byteBuf.readByte() & BIT_8L) << 16)
                | ((byteBuf.readByte() & BIT_8L) << 24)
                | ((byteBuf.readByte() & BIT_8L) << 32)
                | ((byteBuf.readByte() & BIT_8L) << 40);
    }

    public static long getInt6(ByteBuf byteBuf, int index) {
        return (byteBuf.getByte(index++) & BIT_8L)
                | ((byteBuf.getByte(index++) & BIT_8L) << 8)
                | ((byteBuf.getByte(index++) & BIT_8L) << 16)
                | ((byteBuf.getByte(index++) & BIT_8L) << 24)
                | ((byteBuf.getByte(index++) & BIT_8L) << 32)
                | ((byteBuf.getByte(index) & BIT_8L) << 40);
    }


    public static long readInt8(ByteBuf byteBuf) {
        return byteBuf.readLongLE();
    }

    public static BigInteger readInt8AsBigInteger(final ByteBuf buffer) {
        final byte[] bytes = new byte[9];
        bytes[0] = 0;
        for (int i = bytes.length - 1; i > 0; i--) {
            bytes[i] = buffer.readByte();
        }
        return new BigInteger(bytes);
    }

    public static long getInt8(ByteBuf byteBuf, int index) {
        return byteBuf.getLongLE(index);
    }

    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static long getLenEnc(ByteBuf byteBuf, int index) {
        final int sw = getInt1AsInt(byteBuf, index++);
        long int8;
        switch (sw) {
            case ENC_0:
                // represents a NULL in a ProtocolText::ResultsetRow
                int8 = NULL_LENGTH;
                break;
            case ENC_3:
                int8 = getInt2AsInt(byteBuf, index);
                break;
            case ENC_4:
                int8 = getInt3(byteBuf, index);
                break;
            case ENC_9:
                int8 = getInt8(byteBuf, index);
                break;
            default:
                // ENC_1
                int8 = sw;

        }
        return int8;
    }

    public static int getHeaderFlag(final ByteBuf cumulateBuffer) {
        return cumulateBuffer.getByte(cumulateBuffer.readerIndex() + HEADER_SIZE) & BIT_8;
    }

    /**
     * @param buffer at least one byte.
     * @return -1 : more cumulate.
     */
    public static long getLenEncTotalByteLength(ByteBuf buffer) {
        int index = buffer.readerIndex();
        final int sw = getInt1AsInt(buffer, index++);
        final long totalLength;
        switch (sw) {
            case ENC_0:
                // represents a NULL in a ProtocolText::ResultsetRow
                totalLength = 1L;
                break;
            case ENC_3: {
                if (buffer.readableBytes() < 3) {
                    totalLength = -1L;
                } else {
                    totalLength = 3L + getInt2AsInt(buffer, index);
                }
            }
            break;
            case ENC_4: {
                if (buffer.readableBytes() < 4) {
                    totalLength = -1L;
                } else {
                    totalLength = 4L + getInt3(buffer, index);
                }
            }
            break;
            case ENC_9: {
                if (buffer.readableBytes() < 9) {
                    totalLength = -1L;
                } else {
                    totalLength = 9L + getInt8(buffer, index);
                }
            }
            break;
            default:
                // ENC_1
                totalLength = 1L + sw;

        }
        return totalLength;
    }

    public static int obtainIntLenEncLength(final long intLenEnc) {
        final int length;
        if (intLenEnc >= 0 && intLenEnc < ENC_0) {
            length = 1;
        } else if (intLenEnc >= ENC_0 && intLenEnc < (1 << 16)) {
            length = 3;
        } else if (intLenEnc >= (1 << 16) && intLenEnc < (1 << 24)) {
            length = 4;
        } else {
            // intLenEnc < 0 || intLenEnc >=  (1 << 24)
            length = 9;
        }
        return length;
    }


    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static long readLenEnc(ByteBuf byteBuf) {
        final int sw = readInt1AsInt(byteBuf);
        long int8;
        switch (sw) {
            case ENC_0:
                // represents a NULL in a ProtocolText::ResultsetRow
                int8 = NULL_LENGTH;
                break;
            case ENC_3:
                int8 = readInt2AsInt(byteBuf);
                break;
            case ENC_4:
                int8 = readInt3(byteBuf);
                break;
            case ENC_9:
                int8 = readInt8(byteBuf);
                break;
            default:
                // ENC_1
                int8 = sw;

        }
        return int8;
    }

    /**
     * see {@code com.mysql.cj.protocol.a.NativePacketPayload#readInteger(com.mysql.cj.protocol.a.NativeConstants.IntegerDataType)}
     */
    public static int readLenEncAsInt(ByteBuf byteBuf) {
        long intEnc = readLenEnc(byteBuf);
        if (intEnc > Integer.MAX_VALUE) {
            throw new JdbdException("length encode integer cant' convert to int.");
        }
        return (int) intEnc;
    }


    /**
     * Protocol::NulTerminatedString
     * Strings that are terminated by a [00] byte.
     */
    public static String readStringTerm(ByteBuf byteBuf, Charset charset) {
        return new String(readStringTermBytes(byteBuf), charset);
    }

    public static byte[] readStringTermBytes(ByteBuf byteBuf) {
        int index = byteBuf.readerIndex();
        int end = byteBuf.writerIndex();
        while ((index < end) && (byteBuf.getByte(index) != 0)) {
            index++;
        }
        if (index >= end) {
            throw new IndexOutOfBoundsException(String.format("not found [00] byte,index:%s,writerIndex:%s", index, end));
        }
        byte[] bytes = new byte[index - byteBuf.readerIndex()];
        byteBuf.readBytes(bytes);
        byteBuf.readByte();// skip terminating byte
        return bytes;
    }


    public static String readStringFixed(ByteBuf byteBuf, int len, Charset charset) {
        byte[] bytes = new byte[len];
        byteBuf.readBytes(bytes);
        return new String(bytes, charset);
    }

    /**
     * Protocol::RestOfPacketString
     * If a string is the last component of a packet, its length can be calculated from the overall packet length minus the current position.
     */
    public static String readStringEof(ByteBuf byteBuf, int payloadLength, Charset charset) {
        // byteBuf is full packet.
        return readStringFixed(byteBuf, payloadLength, charset);
    }

    public static String readStringEof(ByteBuf byteBuf, Charset charset) {
        return readStringFixed(byteBuf, byteBuf.readableBytes(), charset);
    }


    /**
     * Protocol::LengthEncodedString
     * A length encoded string is a string that is prefixed with length encoded integer describing the length of the string.
     * It is a special case of Protocol::VariableLengthString
     */
    @Nullable
    public static String readStringLenEnc(ByteBuf byteBuf, Charset charset) {
        final byte[] bytes = readBytesLenEnc(byteBuf);
        final String text;
        if (bytes == null) {
            text = null;
        } else if (bytes.length == 0) {
            text = "";
        } else {
            text = new String(bytes, charset);
        }
        return text;
    }

    /**
     * @see #readStringLenEnc(ByteBuf, Charset)
     */
    @Nullable
    public static byte[] readBytesLenEnc(ByteBuf byteBuf) {
        final int len = readLenEncAsInt(byteBuf);
        final byte[] bytes;
        if (len == NULL_LENGTH) {
            bytes = null;
        } else if (len == 0L) {
            bytes = EMPTY_BYTE_ARRAY;
        } else {
            bytes = new byte[len];
            byteBuf.readBytes(bytes);
        }
        return bytes;
    }


    /**
     * <p>
     * This method does not modify {@code readerIndex} or {@code writerIndex} of
     * this buffer.
     * </p>
     *
     * @return true ,at least have one packet.
     */
    public static boolean hasOnePacket(final ByteBuf cumulateBuffer) {
        final int readableBytes = cumulateBuffer.readableBytes();
        return readableBytes > HEADER_SIZE
                && (readableBytes >= HEADER_SIZE + getInt3(cumulateBuffer, cumulateBuffer.readerIndex()));
    }

    /**
     * @return payload byte number of big packet.
     */
    public static int payloadBytesOfBigPacket(final ByteBuf cumulateBuffer) {
        final int originalIndex;
        originalIndex = cumulateBuffer.readerIndex();

        int totalPayloadBytes = 0;

        for (int payloadLength; hasOnePacket(cumulateBuffer); ) {
            payloadLength = readInt3(cumulateBuffer);
            cumulateBuffer.skipBytes(1 + payloadLength);

            totalPayloadBytes += payloadLength;

            if (payloadLength < MAX_PAYLOAD) {
                //big packet end
                break;
            }

        }

        cumulateBuffer.readerIndex(originalIndex);
        return totalPayloadBytes;
    }

    /**
     * @return sequenceId
     */
    public static int readBigPayload(final ByteBuf cumulateBuffer, final ByteBuf payload) {
        int sequenceId = -1;
        for (int payloadLength; hasOnePacket(cumulateBuffer); ) {
            payloadLength = readInt3(cumulateBuffer);
            sequenceId = Packets.readInt1AsInt(cumulateBuffer);

            if (payloadLength > 0) {
                cumulateBuffer.readBytes(payload, payloadLength);
            }
            if (payloadLength < MAX_PAYLOAD) {
                break;
            }
        }
        if (sequenceId < 0) {
            //no bug,never here
            throw new IllegalArgumentException("non big packet");
        }
        return sequenceId;
    }

    /**
     * @param packetNumber not negative.
     */
    public static boolean hasPacketNumber(final ByteBuf cumulateBuffer, final int packetNumber) {
        if (packetNumber < 0) {
            throw new IllegalArgumentException("packetNumber less than zero.");
        }
        final int originalReaderIndex = cumulateBuffer.readerIndex();
        int packetCount = 0;
        for (int payloadLength; Packets.hasOnePacket(cumulateBuffer); ) {
            payloadLength = Packets.readInt3(cumulateBuffer);
            cumulateBuffer.skipBytes(1 + payloadLength);
            if (payloadLength < MAX_PAYLOAD) {
                packetCount++;
            }
            if (packetCount >= packetNumber) {
                break;
            }
        }
        cumulateBuffer.readerIndex(originalReaderIndex);
        return packetCount >= packetNumber;
    }


    static void writeHeader(final ByteBuf packetBuf, final int sequenceId) {
        final int headerSize = HEADER_SIZE, readableBytes;
        readableBytes = packetBuf.readableBytes();
        if (readableBytes < headerSize || readableBytes > MAX_PACKET) {
            throw new IllegalArgumentException(String.format("packetBuf readableBytes[%s] error.", readableBytes));
        }
        final int originalWriterIndex;
        originalWriterIndex = packetBuf.writerIndex();
        packetBuf.writerIndex(packetBuf.readerIndex());

        writeInt3(packetBuf, readableBytes - headerSize);
        packetBuf.writeByte(sequenceId);

        packetBuf.writerIndex(originalWriterIndex);
    }

    /**
     * @return publish length of payload byte
     */
    public static int publishBigPayload(final ByteBuf bigPayload, FluxSink<ByteBuf> sink
            , Supplier<Integer> sequenceIdSupplier, Function<Integer, ByteBuf> bufferCreator
            , final boolean publishSmallPacket) {
        ByteBuf packet;
        int publishLength = 0;
        for (int readableBytes; ; ) {
            readableBytes = bigPayload.readableBytes();
            if (readableBytes >= MAX_PAYLOAD) {
                packet = bufferCreator.apply(MAX_PACKET);

                writeInt3(packet, MAX_PAYLOAD);
                packet.writeByte(sequenceIdSupplier.get());
                packet.writeBytes(bigPayload, MAX_PAYLOAD);

                sink.next(packet);
                publishLength += MAX_PAYLOAD;
            } else {
                if (publishSmallPacket) {
                    packet = bufferCreator.apply(HEADER_SIZE + readableBytes);

                    writeInt3(packet, readableBytes);
                    packet.writeByte(sequenceIdSupplier.get());
                    if (readableBytes > 0) {
                        packet.writeBytes(bigPayload, readableBytes);
                    }
                    sink.next(packet);

                    bigPayload.release();
                    publishLength += readableBytes;
                }
                break;
            }
        }

        return publishLength;
    }


    /**
     * @return publish length of payload byte
     */
    public static int publishBigPacket(final ByteBuf bigPacket, FluxSink<ByteBuf> sink
            , Supplier<Integer> sequenceIdSupplier, Function<Integer, ByteBuf> bufferCreator
            , final boolean publishSmallPacket) {
        int readableBytes = bigPacket.readableBytes();
        if (readableBytes < HEADER_SIZE) {
            throw new IllegalArgumentException(String.format("bigPacket readableBytes[%s]", readableBytes));
        }

        int publishLength = 0;

        ByteBuf packet;
        if (readableBytes >= MAX_PACKET) {
            if (bigPacket.isReadOnly()) {
                packet = bufferCreator.apply(MAX_PACKET);
                packet.writeBytes(bigPacket, MAX_PACKET);
            } else {
                packet = bigPacket.readRetainedSlice(MAX_PACKET);
            }
            writeHeader(packet, sequenceIdSupplier.get());
            sink.next(packet);

            publishLength += MAX_PAYLOAD;
        }
        if (bigPacket.readableBytes() >= MAX_PAYLOAD) {
            publishLength += publishBigPayload(bigPacket, sink, sequenceIdSupplier
                    , bufferCreator, publishSmallPacket);
        } else if (publishSmallPacket) {
            readableBytes = bigPacket.readableBytes();
            packet = bufferCreator.apply(HEADER_SIZE + readableBytes);

            writeInt3(packet, readableBytes);
            packet.writeByte(sequenceIdSupplier.get());
            if (readableBytes > 0) {
                packet.writeBytes(bigPacket, readableBytes);
            }

            sink.next(packet);

            bigPacket.release();
            publishLength += readableBytes;
        }
        return publishLength;
    }

    /**
     * <ol>
     *     <li>bigPacket will be send.</li>
     *     <li>suffix that it's length of payload less than {@link #MAX_PAYLOAD} will be cut to new packet.</li>
     *     <li>bigPacket {@link ByteBuf#release()} will invoked.</li>
     * </ol>
     *
     * @return packet than it's length of payload less than {@link #MAX_PAYLOAD}.
     */
    public static ByteBuf publishAndCutBigPacket(final ByteBuf bigPacket, FluxSink<ByteBuf> sink
            , Supplier<Integer> sequenceIdSupplier, Function<Integer, ByteBuf> bufferCreator) {

        publishBigPacket(bigPacket, sink, sequenceIdSupplier, bufferCreator, false);

        final ByteBuf tempBuffer;
        final int readableBytes = bigPacket.readableBytes();
        if (readableBytes > 0) {
            tempBuffer = bufferCreator.apply(readableBytes);
        } else {
            tempBuffer = bufferCreator.apply(128);
        }
        tempBuffer.writeBytes(bigPacket);
        bigPacket.release();

        return tempBuffer;
    }

    public static ByteBuf addAndCutBigPacket(final ByteBuf bigPacket, final List<ByteBuf> packetList
            , Supplier<Integer> sequenceIdSupplier, Function<Integer, ByteBuf> bufferCreator) {
        if (bigPacket.readableBytes() < MAX_PACKET) {
            return bigPacket;
        }

        ByteBuf packet;
        if (bigPacket.isReadOnly()) {
            packet = bufferCreator.apply(MAX_PACKET);

            writeInt3(packet, MAX_PAYLOAD);
            packet.writeByte(sequenceIdSupplier.get());
            bigPacket.skipBytes(HEADER_SIZE); // bigPacket skip header part
            packet.writeBytes(bigPacket, MAX_PAYLOAD);
        } else {
            packet = bigPacket.readRetainedSlice(MAX_PACKET);
            Packets.writeHeader(packet, sequenceIdSupplier.get());
        }
        packetList.add(packet);

        while (bigPacket.readableBytes() >= MAX_PAYLOAD) {
            packet = bufferCreator.apply(MAX_PACKET);

            writeInt3(packet, MAX_PAYLOAD);
            packet.writeByte(sequenceIdSupplier.get());
            packet.writeBytes(bigPacket, MAX_PAYLOAD);

            packetList.add(packet);
        }

        if (bigPacket.readableBytes() > 0) {
            packet = bufferCreator.apply(HEADER_SIZE + bigPacket.readableBytes());
            packet.writeZero(HEADER_SIZE);
            packet.writeBytes(bigPacket);
        } else {
            packet = bufferCreator.apply(1024);
            packet.writeZero(HEADER_SIZE);
        }

        bigPacket.release();
        return packet;
    }


    /**
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    @Deprecated
    public static Publisher<ByteBuf> createSimpleCommand(final byte cmdFlag, String sql
            , TaskAdjutant adjutant, Supplier<Integer> sequenceId) {

        if (cmdFlag != COM_QUERY && cmdFlag != COM_STMT_PREPARE) {
            throw new IllegalArgumentException("command error");
        }
        final byte[] commandArray = sql.getBytes(adjutant.charsetClient());
        final int maxAllowedPayload = adjutant.getFactory().maxAllowedPacket;
        final int actualPayload = commandArray.length + 1;

        if (actualPayload < 0 || actualPayload > maxAllowedPayload) {
            throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
        }
        final ByteBufAllocator allocator = adjutant.allocator();
        final Publisher<ByteBuf> publisher;
        if (actualPayload < MAX_PAYLOAD) {
            final ByteBuf packet = allocator.buffer(HEADER_SIZE + actualPayload);
            writeInt3(packet, actualPayload);
            packet.writeByte(sequenceId.get());

            packet.writeByte(cmdFlag);
            packet.writeBytes(commandArray);

            publisher = Mono.just(packet);
        } else {
            final List<ByteBuf> list = new ArrayList<>();
            ByteBuf packet = allocator.buffer(MAX_PACKET);
            writeInt3(packet, MAX_PAYLOAD);
            packet.writeByte(sequenceId.get());

            packet.writeByte(cmdFlag);
            int offset = 0, length = MAX_PAYLOAD - 1;
            packet.writeBytes(commandArray, offset, length);
            list.add(packet);
            offset += length;
            for (; offset < commandArray.length; offset += length) {
                length = Math.min(MAX_PAYLOAD, commandArray.length - offset);
                packet = allocator.buffer(HEADER_SIZE + length);
                writeInt3(packet, length);
                packet.writeByte(sequenceId.get());

                packet.writeBytes(commandArray, offset, length);
                list.add(packet);
            }
            if ((list.size() == 1 && actualPayload == MAX_PAYLOAD)
                    || (list.size() > 1 && length == MAX_PAYLOAD)) {
                list.add(createEmptyPacket(allocator, sequenceId.get()));
            }
            publisher = Flux.fromIterable(list);
        }
        return publisher;
    }

    public static ByteBuf createStmtPacket(ByteBufAllocator allocator, Stmt stmt) {
        int capacity = 0;

        if (stmt instanceof SingleStmt) {
            capacity += (((SingleStmt) stmt).getSql().length() + 128);
            if (stmt instanceof ParamStmt) {
                capacity += (((ParamStmt) stmt).getParamGroup().size() * 6);
            } else if (stmt instanceof ParamBatchStmt) {
                capacity += (((ParamBatchStmt) stmt).getGroupList().size() * 10);
            }
        } else if (stmt instanceof StaticMultiStmt) {
            capacity += ((StaticMultiStmt) stmt).getMultiStmt().length();
        } else if (stmt instanceof ParamMultiStmt) {
            capacity += (((ParamMultiStmt) stmt).getStmtList().size() * 50);
        }

        if (capacity < 0) {
            capacity = Integer.MAX_VALUE - 128;
        } else {
            capacity += (stmt.getStmtVarList().size() * 6);
        }

        if (capacity < 0) {
            capacity = Integer.MAX_VALUE - 128;
        }
        final ByteBuf packet = allocator.buffer(capacity, Integer.MAX_VALUE);
        packet.writeZero(HEADER_SIZE); // placeholder of header
        return packet;

    }


    /**
     * @param packet a big packet that header is placeholder.
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    public static Publisher<ByteBuf> createPacketPublisher(final ByteBuf packet, final IntSupplier sequenceId,
                                                           final TaskAdjutant adjutant) {

        final int maxAllowedPayload = adjutant.getFactory().maxAllowedPacket;
        final int payload = packet.readableBytes() - Packets.HEADER_SIZE;
        if (payload > maxAllowedPayload) {
            throw MySQLExceptions.createNetPacketTooLargeException(maxAllowedPayload);
        }
        final Publisher<ByteBuf> publisher;
        if (payload < Packets.MAX_PAYLOAD) {
            Packets.writeHeader(packet, sequenceId.getAsInt());
            publisher = Mono.just(packet);
        } else {
            publisher = Flux.fromIterable(Packets.divideBigPacket(packet, sequenceId));
        }
        return publisher;
    }

    static void sendPackets(final ByteBuf packet, final IntSupplier sequenceId, final FluxSink<ByteBuf> sink) {
        final int readableBytes, payload;
        readableBytes = packet.readableBytes();
        payload = readableBytes - HEADER_SIZE;

        if (payload < MAX_PAYLOAD) {
            Packets.writeHeader(packet, sequenceId.getAsInt());
            sink.next(packet);
        } else {
            for (ByteBuf smallPacket : divideBigPacket(packet, sequenceId)) {
                sink.next(smallPacket);
            }
        }

    }


    /**
     * <p>
     * bigPacket will be release after return.
     * </p>
     *
     * @param bigPacket a big packet that header is placeholder.
     * @return a sync Publisher that is created by {@link Mono#just(Object)} or {@link Flux#fromIterable(Iterable)}.
     */
    static Iterable<ByteBuf> divideBigPacket(final ByteBuf bigPacket, final IntSupplier sequenceId) {
        if (bigPacket.readableBytes() < MAX_PACKET) {
            throw new IllegalArgumentException("bigPacket not big packet");
        }
        final ByteBufAllocator allocator = bigPacket.alloc();
        ByteBuf packet = allocator.buffer(MAX_PACKET);

        int length = MAX_PACKET;
        packet.writeBytes(bigPacket, length);
        writeHeader(packet, sequenceId.getAsInt());

        final List<ByteBuf> packetList = MySQLCollections.arrayList();
        packetList.add(packet);

        while (bigPacket.isReadable()) {
            length = Math.min(MAX_PAYLOAD, bigPacket.readableBytes());
            packet = allocator.buffer(HEADER_SIZE + length);

            packet.writeZero(HEADER_SIZE); // placeholder of header
            packet.writeBytes(bigPacket, length);

            writeHeader(packet, sequenceId.getAsInt());
            packetList.add(packet);

        }
        if (bigPacket.refCnt() > 0) {
            bigPacket.release(); // must release
        }
        if (packetList.size() == 1 || length == MAX_PAYLOAD) {
            packetList.add(createEmptyPacket(allocator, sequenceId.getAsInt()));
        }
        return packetList;
    }


    public static Publisher<ByteBuf> createMultiPacket(ByteBuf multiPacket, Supplier<Integer> sequenceIdSupplier
            , ByteBufAllocator allocator) {

        final Publisher<ByteBuf> publisher;

        if (multiPacket.readableBytes() < Packets.MAX_PACKET) {
            Packets.writeHeader(multiPacket, sequenceIdSupplier.get());
            publisher = Mono.just(multiPacket);
        } else {
            LinkedList<ByteBuf> packetList = new LinkedList<>();

            ByteBuf packet = multiPacket.readRetainedSlice(Packets.MAX_PACKET);
            Packets.writeHeader(packet, sequenceIdSupplier.get());
            packetList.add(packet);

            for (int readableBytes = multiPacket.readableBytes(), payloadLength; readableBytes > 0; ) {
                payloadLength = Math.min(readableBytes, Packets.MAX_PAYLOAD);

                packet = allocator.buffer(Packets.HEADER_SIZE + payloadLength);
                Packets.writeInt3(packet, payloadLength);
                packet.writeByte(sequenceIdSupplier.get());

                packet.writeBytes(multiPacket, payloadLength);
                packetList.add(packet);

                readableBytes = multiPacket.readableBytes();
            }
            packet = packetList.getLast();
            if (packet.readableBytes() == MAX_PACKET) {
                packetList.add(createEmptyPacket(allocator, sequenceIdSupplier.get()));
            }
            publisher = Flux.fromIterable(packetList);
        }
        return publisher;
    }


    public static ByteBuf createEmptyPacket(ByteBufAllocator allocator, int sequenceId) {
        // append empty packet.
        ByteBuf packet = allocator.buffer(HEADER_SIZE);
        writeInt3(packet, 0);
        packet.writeByte(sequenceId);
        return packet;
    }

    static ByteBuf createOnePacket(final ByteBufAllocator allocator, final int initialPayloadCapacity) {
        final int headerSize = HEADER_SIZE;
        final ByteBuf packet;
        packet = allocator.buffer(headerSize + initialPayloadCapacity, (1 << 30) + headerSize);
        packet.writeZero(headerSize);
        return packet;
    }


    public static BigInteger convertInt8ToBigInteger(long int8) {
        BigInteger bigInteger;
        if (int8 < 0) {
            byte[] bytes = new byte[9];
            int index = 0;
            bytes[index++] = 0;
            divideInt8(int8, bytes, index);
            bigInteger = new BigInteger(bytes);
        } else {
            bigInteger = BigInteger.valueOf(int8);
        }
        return bigInteger;
    }


    public static void writeInt1(ByteBuf byteBuffer, final int int1) {
        byteBuffer.writeByte(int1);
    }

    public static void writeInt2(ByteBuf byteBuffer, final int int2) {
        byteBuffer.writeShortLE(int2);
    }

    public static void writeInt3(ByteBuf byteBuffer, final int int3) {
        byteBuffer.writeByte(int3);
        byteBuffer.writeByte((int3 >> 8));
        byteBuffer.writeByte((int3 >> 16));
    }

    public static void writeInt4(ByteBuf byteBuffer, final int int4) {
        byteBuffer.writeIntLE(int4);
    }

    public static void writeInt8(ByteBuf byteBuffer, final long int8) {
        byteBuffer.writeLongLE(int8);
    }

    public static void writeIntLenEnc(ByteBuf packetBuffer, final long intLenEnc) {
        if (intLenEnc >= 0 && intLenEnc < ENC_0) {
            packetBuffer.writeByte((int) intLenEnc);
        } else if (intLenEnc >= ENC_0 && intLenEnc < (1 << 16)) {
            packetBuffer.writeByte(ENC_3);
            writeInt2(packetBuffer, (int) intLenEnc);
        } else if (intLenEnc >= (1 << 16) && intLenEnc < (1 << 24)) {
            packetBuffer.writeByte(ENC_4);
            writeInt3(packetBuffer, (int) intLenEnc);
        } else {
            // intLenEnc < 0 || intLenEnc >=  (1 << 24)
            packetBuffer.writeByte(ENC_9);
            writeInt8(packetBuffer, intLenEnc);
        }
    }

    public static void writeStringTerm(ByteBuf byteBuffer, byte[] stringBytes) {
        byteBuffer.writeBytes(stringBytes);
        byteBuffer.writeZero(1);
    }

    public static void writeStringLenEnc(ByteBuf packetBuffer, ByteBuf stringBuffer) {
        writeIntLenEnc(packetBuffer, stringBuffer.readableBytes());
        packetBuffer.writeBytes(stringBuffer);
    }

    public static void writeStringLenEnc(ByteBuf packetBuffer, byte[] stringBytes) {
        writeIntLenEnc(packetBuffer, stringBytes.length);
        packetBuffer.writeBytes(stringBytes);
    }


    public static void writeBytesAtIndex(final ByteBuf buffer, final byte[] bytes, final int index) {
        final int writableIndex;
        writableIndex = buffer.writerIndex();

        buffer.writerIndex(index);
        buffer.writeBytes(bytes);


        buffer.writerIndex(writableIndex);
    }


    public static boolean isAuthSwitchRequestPacket(ByteBuf payloadBuf) {
        return getInt1AsInt(payloadBuf, payloadBuf.readerIndex()) == 0xFE;
    }

    /*################################## blow private static method ##################################*/

    private static void divideInt8(long int8, byte[] bytes, int index) {
        bytes[index++] = (byte) (int8 >> 56);
        bytes[index++] = (byte) (int8 >> 48);
        bytes[index++] = (byte) (int8 >> 40);
        bytes[index++] = (byte) (int8 >> 32);

        bytes[index++] = (byte) (int8 >> 24);
        bytes[index++] = (byte) (int8 >> 16);
        bytes[index++] = (byte) (int8 >> 8);
        bytes[index] = (byte) int8;
    }


}
