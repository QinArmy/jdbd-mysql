package io.jdbd.postgre.type;

import io.jdbd.type.geometry.Circle;
import io.jdbd.type.geometry.Line;
import io.jdbd.type.geometry.LineString;
import io.jdbd.type.geometry.Point;
import io.jdbd.vendor.type.Geometries;
import io.jdbd.vendor.util.GeometryUtils;
import io.jdbd.vendor.util.JdbdNumbers;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import static org.testng.Assert.*;

/**
 * @see PgGeometries
 */
public class PgGeometriesUnitTests {

    private static final Logger LOG = LoggerFactory.getLogger(PgGeometriesUnitTests.class);

    /**
     * @see PgGeometries#point(String)
     */
    @Test
    public void point() {
        String text;
        Point point;

        text = "(0,0)";
        point = PgGeometries.point(text);
        assertEquals(point.getX(), 0, "x");
        assertEquals(point.getY(), 0, "y");

        text = "( 0,  3.3    )";
        point = PgGeometries.point(text);
        assertEquals(point.getX(), 0, "x");
        assertEquals(point.getY(), 3.3, "y");

        text = String.format("( %s,  %s    )", Double.MAX_VALUE, Double.MIN_VALUE);
        point = PgGeometries.point(text);
        assertEquals(point.getX(), Double.MAX_VALUE, "x");
        assertEquals(point.getY(), Double.MIN_VALUE, "y");
    }

    /**
     * @see PgGeometries#point(String)
     */
    @Test
    public void pointError() {
        String text;

        text = "0,9";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

        text = "(0,9";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

        text = "0,9)";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

        text = "(0 9)";
        try {

            PgGeometries.point(text);
            fail(String.format("pointError[%s] test failure.", text));
        } catch (IllegalArgumentException e) {
            LOG.info("pointError[{}] test success.", text);
        }

    }

    /**
     * @see PgGeometries#readPoints(String, int, BiConsumer)
     */
    @Test
    public void doReadPoints() {
        LOG.info("doReadPoints test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);
        int newIndex;
        final BiConsumer<Double, Double> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            pointsText = " (0, 0 ) , (1, 1)     ";
            newIndex = PgGeometries.readPoints(pointsText, 0, pointConsumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 2 * 16, "point count");
            out.clear();

            pointsText = String.format(" (0, 0 ) , (1, 1),(%s,%s)  ,(454.0,32.2)   ", Double.MAX_VALUE, Double.MIN_VALUE);
            newIndex = PgGeometries.readPoints(pointsText, 0, pointConsumer);

            assertTrue(newIndex > pointsText.lastIndexOf(')'), "newIndex");
            assertEquals(out.readableBytes(), 4 * 16, "point count");
        } finally {
            out.release();
        }

        LOG.info("doReadPoints test success.");
    }

    /**
     * @see PgGeometries#readPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError1() {
        LOG.info("doReadPointsWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,    ";
        final BiConsumer<Double, Double> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            PgGeometries.readPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError1 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError1 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#readPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError2() {
        LOG.info("doReadPointsWithError2 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ()    ";
        final BiConsumer<Double, Double> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            PgGeometries.readPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError2 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError2 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#readPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void doReadPointsWithError3() {
        LOG.info("doReadPointsWithError3 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = " (0, 0 ) , (1, 1) ,(3434,    ";
        final BiConsumer<Double, Double> pointConsumer = PgGeometries.writePointWkbFunction(false, out);
        try {
            PgGeometries.readPoints(pointsText, 0, pointConsumer);
            fail("doReadPointsWithError3 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("doReadPointsWithError3 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }

    /**
     * @see PgGeometries#lineSegmentToWkb(String, boolean)
     */
    @Test
    public void lineSegmentToWbk() {
        LOG.info("lineSegmentToWbk test start.");

        String pointsText;
        byte[] wkb;
        final boolean bigEndian = false;

        pointsText = "[ (0, 0 ) , (1, 1)]";
        wkb = PgGeometries.lineSegmentToWkb(pointsText, bigEndian);
        LOG.info("WKB type:{}", JdbdNumbers.readIntFromEndian(false, wkb, 1, 4));
        final String wkt = "LINESTRING(0 0,1 1)";

        assertTrue(Arrays.equals(wkb, GeometryUtils.lineStringToWkb(wkt, bigEndian)), "wkb error");
        LOG.info("lineSegmentToWbk test success.");
    }

    /**
     * @see PgGeometries#readPoints(String, int, BiConsumer)
     */
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void lineSegmentToWbkWithError1() {
        LOG.info("lineSegmentToWbkWithError1 test start.");
        String pointsText;
        final ByteBuf out = ByteBufAllocator.DEFAULT.buffer(100);

        pointsText = "[ (0, 0 ) , (1, 1) this is evil. ]";

        try {
            PgGeometries.lineSegmentToWkb(pointsText, false);
            fail("lineSegmentToWbkWithError1 test failure.");
        } catch (IllegalArgumentException e) {
            LOG.info("lineSegmentToWbkWithError1 test success. message : {}", e.getMessage());
            throw e;
        } finally {
            out.release();
        }

    }


    /**
     * @see PgLine#from(String)
     */
    @Test
    public void pgLineParse() {
        String text;
        PgLine line;
        double a, b, c;
        a = 0;
        b = 1.3;
        c = 5;
        text = String.format("{%s,%s,%s}", a, b, c);
        line = PgLine.from(text);
        assertEquals(line.toString(), text, "PgLine.toString()");
        assertEquals(line.getA(), a, "a");
        assertEquals(line.getB(), b, "b");
        assertEquals(line.getC(), c, "c");

        a = Double.MIN_VALUE;
        b = Double.MAX_VALUE;
        c = 5;
        text = String.format("{%s,%s,%s}", a, b, c);
        line = PgLine.from(text);
        assertEquals(line.toString(), text, "PgLine.toString()");
        assertEquals(line.getA(), a, "a");
        assertEquals(line.getB(), b, "b");
        assertEquals(line.getC(), c, "c");

    }

    /**
     * @see PgBox#from(String)
     */
    @Test
    public void pgBoxParse() {
        final Point point1 = Geometries.point(1, 1.3), point2 = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);
        String text;
        PgBox box;
        text = String.format("(%s,%s),(%s,%s)", point1.getX(), point1.getY()
                , point2.getX(), point2.getY());

        box = PgBox.from(text);

        assertEquals(box.getPoint1(), point1, "point1");
        assertEquals(box.getPoint2(), point2, "point2");
        assertEquals(box.toString(), text, "toString()");
        assertEquals(box, PgBox.create(point1, point2), "equals()");

    }

    /**
     * @see PgGeometries#lineSegment(String)
     */
    @Test
    public void lineSegment() {
        final Point point1, point2;
        String text;
        Line line;

        point1 = Geometries.point(1.1, 3.3);
        point2 = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);

        text = String.format("[(%s,%s),(%s,%s)]", point1.getX(), point1.getY()
                , point2.getX(), point2.getY());
        line = PgGeometries.lineSegment(text);

        assertEquals(line.getPoint1(), point1, "point1");
        assertEquals(line.getPoint2(), point2, "point2");

        text = String.format("LINESTRING(%s %s,%s %s)", point1.getX(), point1.getY()
                , point2.getX(), point2.getY());

        assertEquals(line.toString(), text, "wkt");

        boolean equals = GeometryUtils.wkbEquals(line.asArray(), GeometryUtils.lineStringToWkb(text, false));
        assertTrue(equals, "wkb");
    }

    /**
     * @see PgGeometries#path(String)
     */
    @Test
    public void path() {

        Point p1, p2;
        p1 = Geometries.point(0, 0);
        p2 = Geometries.point(1, 1);

        doPathTest(p1, p2, "[(%s,%s),(%s,%s)]");
        doPathTest(p1, p2, "((%s,%s),(%s,%s))");

        p1 = Geometries.point(0.3, Double.MIN_VALUE);
        p2 = Geometries.point(1.34, Double.MAX_VALUE);

        doPathTest(p1, p2, "[(%s,%s),(%s,%s)]");
        doPathTest(p1, p2, "((%s,%s),(%s,%s))");

    }

    @Test
    public void polygons() {
        String text;
        Point p1, p2;
        p1 = Geometries.point(0.3, Double.MIN_VALUE);
        p2 = Geometries.point(1.34, Double.MAX_VALUE);

        text = String.format("((%s,%s),(%s,%s))", p1.getX(), p1.getY(), p2.getX(), p2.getY());
        PgPolygon pgPolygon = PgPolygon.wrap(text);

        List<Point> pointList = pgPolygon.toPoints()
                .collectList()
                .block();
        assertNotNull(pointList, "pointList");
        assertFalse(pointList.isEmpty(), "pointList empty");

        assertEquals(pointList.get(0), p1, "p1");
        assertEquals(pointList.get(1), p2, "p2");
    }

    @Test
    public void circle() {
        String text;
        Point p;
        double r;
        p = Geometries.point(Double.MAX_VALUE, Double.MIN_VALUE);
        r = 10;
        text = String.format("<(%s,%s),%s>", p.getX(), p.getY(), r);

        Circle c = PgGeometries.circle(text);
        assertEquals(c.getCenter(), p, "center");
        assertEquals(c.getRadius(), r, "radius");
    }

    private void doPathTest(Point p1, Point p2, String format) {
        String text, wkt, expectedWkt;
        LineString lineString;
        byte[] wkb, expectedWkb;

        text = String.format(format, p1.getX(), p1.getY(), p2.getX(), p2.getY());
        lineString = PgGeometries.path(text);

        wkt = lineString.toWkt();
        expectedWkt = String.format("LINESTRING(%s %s,%s %s)", p1.getX(), p1.getY(), p2.getX(), p2.getY());
        assertEquals(wkt, expectedWkt, "wkt");

        wkb = lineString.toWkb();
        expectedWkb = GeometryUtils.lineToWkb(p1, p2, true);
        assertEquals(wkb, expectedWkb, "wkb");

        List<Point> pointList = Flux.from(lineString.points())
                .collectList()
                .block();

        assertNotNull(pointList, "pointList");
        assertFalse(pointList.isEmpty(), "pointList empty");

        assertEquals(pointList.get(0), p1, "p1");
        assertEquals(pointList.get(1), p2, "p2");
    }


}
