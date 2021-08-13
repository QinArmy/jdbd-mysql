package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.type.Geometries;
import io.jdbd.vendor.util.GeometryUtils;
import reactor.core.publisher.Flux;

import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * @see <a href="https://www.postgresql.org/docs/current/datatype-geometric.html#DATATYPE-LSEG">Lines</a>
 */
final class PgLineSegment implements Line {

    static PgLineSegment from(final String textValue)
            throws IllegalArgumentException {
        if (!textValue.startsWith("[") || !textValue.endsWith("]")) {
            throw PgGeometries.createGeometricFormatError(textValue);
        }
        final Point[] points = new Point[2];
        final BiConsumer<Double, Double> pointConsumer = (x, y) -> {
            if (points[0] == null) {
                points[0] = Geometries.point(x, y);
            } else if (points[1] == null) {
                points[1] = Geometries.point(x, y);
            } else {
                throw PgGeometries.createGeometricFormatError(textValue);
            }
        };

        final int newIndex;
        newIndex = PgGeometries.doReadPoints(textValue, 1, pointConsumer);

        if (points[1] == null) {
            throw PgGeometries.createGeometricFormatError(textValue);
        } else {
            PgGeometries.checkPgGeometricSuffix(textValue, newIndex);
        }
        return new PgLineSegment(textValue, points[0], points[1]);
    }

    private final String textValue;

    private final Point point1;

    private final Point point2;

    private PgLineSegment(String textValue, Point point1, Point point2) {
        this.textValue = textValue;
        this.point1 = point1;
        this.point2 = point2;
    }

    @Override
    public final Point getPoint1() {
        return this.point1;
    }

    @Override
    public final Point getPoint2() {
        return this.point2;
    }


    @Override
    public byte[] toWkb(boolean bigEndian) throws IllegalStateException {
        return GeometryUtils.lineToWkb(this.point1, this.point2, bigEndian);
    }


    @Override
    public final boolean isArray() {
        return true;
    }

    @Override
    public final byte[] asArray() throws IllegalStateException {
        return toWkb(false);
    }


    @Override
    public final Flux<Point> points() {
        return Flux.just(this.point1, this.point2);
    }

    @Override
    public final String toWkt() {
        return GeometryUtils.lineToWkt(this.point1, this.point2);
    }

    @Override
    public final FileChannel openReadOnlyChannel() throws IllegalStateException {
        throw new IllegalStateException("Non-underlying file,use asArray() method.");
    }

    @Override
    public final int hashCode() {
        return Objects.hash(this.point1, this.point2);
    }

    @Override
    public final boolean equals(Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof Line) {
            Line line = (Line) obj;
            match = this.point1.equals(line.getPoint1())
                    && this.point2.equals(line.getPoint2());
        } else if (obj instanceof LineString) {
            match = GeometryUtils.lineStringEquals(this, (LineString) obj);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return this.textValue;
    }


}
