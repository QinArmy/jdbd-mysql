package io.jdbd.vendor.util;

import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.type.geometry.WkbType;
import io.jdbd.vendor.type.Geometries;
import org.qinarmy.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


/**
 * This class design for geometry sql type test.
 *
 * @see <a href="https://www.ogc.org/standards/sfa">Simple Feature Access - Part 1: Common Architecture PDF</a>
 */
public abstract class GeometryUtils extends GenericGeometries {


    private final static Logger LOG = LoggerFactory.getLogger(GeometryUtils.class);

    public static final byte WKB_POINT_BYTES = 21;

    public static String pointToWkt(Point point) {
        return new StringBuilder("POINT(")
                .append(point.getX())
                .append(" ")
                .append(point.getY())
                .toString();
    }

    public static byte[] pointToWkb(Point point, boolean bigEndian) {
        byte[] wkb = new byte[21];
        int offset = 0;

        wkb[offset++] = (byte) (bigEndian ? 0 : 1);
        JdbdNumbers.intToEndian(bigEndian, WkbType.POINT.code, wkb, offset, 4);
        offset += 4;

        JdbdNumbers.doubleToEndian(bigEndian, point.getX(), wkb, offset);
        offset += 8;
        JdbdNumbers.doubleToEndian(bigEndian, point.getY(), wkb, offset);
        return wkb;
    }

    public static String lineToWkt(Point point1, Point point2) {
        return String.format("LINESTRING(%s %s,%s %s)"
                , point1.getX(), point1.getY()
                , point2.getX(), point2.getY());
    }

    public static byte[] lineToWkb(final Point point1, final Point point2, final boolean bigEndian) {
        final byte[] wkb = new byte[41];
        int offset = 0;

        wkb[offset++] = (byte) (bigEndian ? 0 : 1);
        JdbdNumbers.intToEndian(bigEndian, WkbType.LINE_STRING.code, wkb, offset, 4);
        offset += 4;
        JdbdNumbers.intToEndian(bigEndian, 2, wkb, offset, 4);
        offset += 4;

        JdbdNumbers.doubleToEndian(bigEndian, point1.getX(), wkb, offset);
        offset += 8;
        JdbdNumbers.doubleToEndian(bigEndian, point1.getY(), wkb, offset);
        offset += 8;

        JdbdNumbers.doubleToEndian(bigEndian, point2.getX(), wkb, offset);
        offset += 8;
        JdbdNumbers.doubleToEndian(bigEndian, point2.getY(), wkb, offset);
        return wkb;
    }

    public static boolean lineStringEquals(LineString ls1, LineString ls2) {
        // TODO zoro
        throw new UnsupportedOperationException();
    }

    public static Flux<Point> lineStringToPoints(final byte[] wkbArray) {
        final int pintCount = checkLineStringWkb(wkbArray, WkbType.LINE_STRING);
        final boolean bigEndian = wkbArray[0] == 0;

        return Flux.create(sink -> {
            double x, y;
            final int endIndex = 9 + (pintCount << 4);

            try {
                for (int i = 9; i < endIndex; ) {
                    x = JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, i, 8);
                    i += 8;
                    y = JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, i, 8);
                    i += 8;
                    sink.next(Geometries.point(x, y));
                }
                sink.complete();
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });

    }

    public static Flux<Point> lineStringToPoints(final Path path) {
        return Flux.create(sink -> {
            try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
                final long totalSize = channel.size();
                final byte[] array = new byte[1024];
                final ByteBuffer buffer = ByteBuffer.wrap(array);
                if (channel.read(buffer) < 9) {
                    sink.error(JdbdExceptions.wrap(new IOException(String.format("path[%s] size < 9", path))));
                    return;
                }
                buffer.flip();
                int offset = buffer.position(), limit = buffer.limit();

                if (WkbType.fromWkbArray(array, offset) != WkbType.LINE_STRING) {
                    sink.error(JdbdExceptions.wrap(new IOException(String.format("path[%s] non LINESTRING .", path))));
                    return;
                }
                final boolean endian = readEndian(array[offset]);
                offset += 5;
                final long count = JdbdNumbers.readIntFromEndian(endian, array, offset, 4) & 0xFFFF_FFFFL;
                if (totalSize != (9 + (count << 4))) {
                    sink.error(JdbdExceptions.wrap(new IOException(String.format("path[%s] non LINESTRING.", path))));
                    return;
                }
                offset += 4;
                long actualCount = 0L;
                while (true) {
                    final int endIndex = offset + ((limit - offset) << 4);
                    double x, y;
                    for (int i = offset; i < endIndex; i++) {
                        x = JdbdNumbers.readDoubleFromEndian(endian, array, i, 8);
                        i += 8;
                        y = JdbdNumbers.readDoubleFromEndian(endian, array, i, 8);
                        i += 8;
                        sink.next(Geometries.point(x, y));
                        actualCount++;
                    }

                    JdbdBuffers.cumulateBuffer(buffer);
                    if (channel.read(buffer) < 1) {
                        break;
                    }
                    buffer.flip();
                }

                if (count == actualCount) {
                    sink.complete();
                } else {
                    String m = String.format("path[%s] expect point count[%s] but %s.", path, count, actualCount);
                    sink.error(JdbdExceptions.wrap(new IOException(m)));
                }
            } catch (Throwable e) {
                sink.error(JdbdExceptions.wrap(e));
            }
        });
    }


    public static byte[] geometryToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));

        final WkbType wkbType = readWkbFromWkt(inBuffer);
        if (wkbType == null) {
            throw new IllegalArgumentException("wktText not geometry.");
        }
        final byte[] wkbArray;
        switch (wkbType) {
            case POINT:
                wkbArray = pointToWkb(wktText, bigEndian);
                break;
            case LINE_STRING:
                wkbArray = lineStringToWkb(wktText, bigEndian);
                break;
            case POLYGON:
                wkbArray = polygonToWkb(wktText, bigEndian);
                break;
            case MULTI_POINT:
                wkbArray = multiPointToWkb(wktText, bigEndian);
                break;
            case MULTI_LINE_STRING:
                wkbArray = multiLineStringToWkb(wktText, bigEndian);
                break;
            case MULTI_POLYGON:
                wkbArray = multiPolygonToWkb(wktText, bigEndian);
                break;
            case GEOMETRY_COLLECTION:
                wkbArray = geometryCollectionToWkb(wktText, bigEndian);
                break;
            default:
                throw createUnsupportedWkb(wkbType);
        }
        return wkbArray;
    }

    /**
     * @param pointWkt like POINT(0,0)
     */
    public static byte[] pointToWkb(final String pointWkt, final boolean bigEndian) {
        return doPointToWkb(pointWkt, bigEndian, true);
    }

    /**
     * @param pointValue like (0,0)
     */
    public static byte[] pointValueToWkb(String pointValue, final boolean bigEndian) {
        return doPointToWkb(pointValue, bigEndian, false);
    }


    public static Pair<Double, Double> readPointAsPair(final byte[] wkbArray, int offset) {
        if (offset < 0 || offset >= wkbArray.length) {
            throw createOffsetError(offset, wkbArray.length);
        }
        if (wkbArray.length - offset < WKB_POINT_BYTES) {
            throw createIllegalWkbLengthError(wkbArray.length, offset + WKB_POINT_BYTES);
        }

        final boolean bigEndian = checkByteOrder(wkbArray[offset++]) == 0;
        final int wkbType = JdbdNumbers.readIntFromEndian(bigEndian, wkbArray, offset, 4);
        if (wkbType != WkbType.POINT.code) {
            throw createWktFormatError(WkbType.POINT.name());
        }
        offset += 4;
        final double x, y;
        x = JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        offset += 8;
        y = JdbdNumbers.readDoubleFromEndian(bigEndian, wkbArray, offset, 8);
        return new Pair<>(x, y);
    }


    public static byte[] lineStringToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.LINE_STRING;
        readNonNullWkb(inBuffer, expectType);

        final int pointCount, headerIndex;
        headerIndex = writeGeometryHeader(outWrapper, expectType);
        pointCount = lineStringTextToWkb(inBuffer, outWrapper, expectType);
        assertWhitespaceSuffix(inBuffer, expectType);

        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, headerIndex, outWrapper.bigEndian, pointCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] polygonToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wkbType = WkbType.POLYGON;
        readNonNullWkb(inBuffer, wkbType);

        final int writerIndex, linearRingCount;
        writerIndex = writeGeometryHeader(outWrapper, wkbType);
        linearRingCount = polygonTextToWkb(inBuffer, outWrapper, wkbType);
        assertWhitespaceSuffix(inBuffer, wkbType);
        if (linearRingCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, linearRingCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }


    public static byte[] multiPointToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.MULTI_POINT;
        readNonNullWkb(inBuffer, expectType);

        final int writerIndex, pointCount;
        writerIndex = writeGeometryHeader(outWrapper, expectType);
        pointCount = multiPointTextToWkb(inBuffer, outWrapper, expectType);
        assertWhitespaceSuffix(inBuffer, expectType);

        if (pointCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, pointCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] multiLineStringToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType expectType = WkbType.MULTI_LINE_STRING;
        readNonNullWkb(inBuffer, expectType);

        final int writerIndex, elementCount;

        writerIndex = writeGeometryHeader(outWrapper, expectType);
        elementCount = multiLineStringTextToWkb(inBuffer, outWrapper, expectType);
        assertWhitespaceSuffix(inBuffer, expectType);
        if (elementCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, elementCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    public static byte[] multiPolygonToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wkbType = WkbType.MULTI_POLYGON;
        readNonNullWkb(inBuffer, wkbType);

        final int writerIndex, polygonCount;

        writerIndex = writeGeometryHeader(outWrapper, wkbType);
        polygonCount = multiPolygonTextToWkb(inBuffer, outWrapper, wkbType);
        assertWhitespaceSuffix(inBuffer, wkbType);
        if (polygonCount != 0) {
            writeInt(outWrapper.outChannel, writerIndex, bigEndian, polygonCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    /**
     * @param wktText <ul>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION}</li>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION_Z}</li>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION_M}</li>
     *                <li>{@link WkbType#GEOMETRY_COLLECTION_ZM}</li>
     *                </ul>
     */
    public static byte[] geometryCollectionToWkb(final String wktText, final boolean bigEndian) {
        ByteBuffer inBuffer = ByteBuffer.wrap(wktText.getBytes(StandardCharsets.US_ASCII));
        WkbMemoryWrapper outWrapper = new WkbMemoryWrapper(256, bigEndian);

        final WkbType wktType = WkbType.GEOMETRY_COLLECTION;
        readNonNullWkb(inBuffer, wktType);

        final int elementCountWriterIndex, geometryCount;
        elementCountWriterIndex = writeGeometryHeader(outWrapper, wktType);
        geometryCount = geometryCollectionTextToWkb(inBuffer, outWrapper, wktType);

        assertWhitespaceSuffix(inBuffer, wktType);
        if (geometryCount != 0) {
            writeInt(outWrapper.outChannel, elementCountWriterIndex, outWrapper.bigEndian, geometryCount);
        }
        return channelToByteArray(outWrapper.outChannel);
    }

    /*################################## blow protected method ##################################*/



    /*################################## blow private method ##################################*/

    /**
     * @see #pointToWkb(String, boolean)
     * @see #pointValueToWkb(String, boolean)
     */
    private static byte[] doPointToWkb(final String pointWkt, final boolean bigEndian, final boolean hasTag) {
        final ByteBuffer inBuffer = ByteBuffer.wrap(pointWkt.getBytes(StandardCharsets.US_ASCII));

        final WkbOUtWrapper outWrapper;
        outWrapper = new WkbOUtWrapper(WKB_POINT_BYTES, bigEndian);

        final WkbType wkbType = WkbType.POINT;
        if (hasTag) {
            readNonNullWkb(inBuffer, wkbType);
        }

        outWrapper.buffer.put(0, outWrapper.bigEndian ? (byte) 0 : (byte) 1);
        JdbdNumbers.intToEndian(outWrapper.bigEndian, wkbType.code, outWrapper.buffer.array(), 1, 4);
        outWrapper.buffer.position(5);

        int pointCount;
        pointCount = pointTextToWkb(inBuffer, outWrapper, wkbType);
        assertWhitespaceSuffix(inBuffer, wkbType);

        outWrapper.buffer.flip();

        final byte[] wkbArray;
        if (pointCount == 1) {
            wkbArray = outWrapper.buffer.array();
        } else {
            wkbArray = new byte[5];
            outWrapper.buffer.get(wkbArray);
        }
        return wkbArray;
    }


}
