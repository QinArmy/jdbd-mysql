package io.jdbd.vendor.type;

import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.util.GeometryUtils;
import reactor.core.publisher.Flux;

import java.nio.channels.FileChannel;
import java.util.Objects;

final class LineImpl implements Line {

    static LineImpl create(Point point1, Point point2) {
        return new LineImpl(point1, point2);
    }

    private final Point point1;

    private final Point point2;

    private LineImpl(Point point1, Point point2) {
        this.point1 = Objects.requireNonNull(point1, "point1");
        this.point2 = Objects.requireNonNull(point2, "point2");
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
    public final Flux<Point> points() {
        return Flux.just(this.point1, this.point2);
    }

    @Override
    public final boolean isArray() {
        return true;
    }

    @Override
    public final String toWkt() {
        return GeometryUtils.lineToWkt(this.point1, this.point2);
    }

    @Override
    public final byte[] toWkb(boolean bigEndian) throws IllegalStateException {
        return GeometryUtils.lineToWkb(this.point1, this.point2, bigEndian);
    }

    @Override
    public final byte[] asArray() throws IllegalStateException {
        return toWkb(false);
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
            Line l = (Line) obj;
            match = this.point1.equals(l.getPoint1())
                    && this.point2.equals(l.getPoint2());
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public final String toString() {
        return toWkt();
    }


}
