package io.jdbd.vendor.result;

import io.jdbd.JdbdException;
import io.jdbd.session.SavePoint;
import io.jdbd.vendor.util.JdbdStrings;

import java.util.Objects;

/**
 * @see NamedSavePoint
 * @see UnNamedSavePoint
 * @since 1.0
 */
public final class JdbdSavePoint implements SavePoint {

    public static SavePoint from(int id, String name) {
        return new JdbdSavePoint(id, name);
    }


    private final int id;

    private final String name;

    /**
     * private constructor
     */
    private JdbdSavePoint(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public boolean isNamed() {
        return true;
    }

    @Override
    public int id() throws JdbdException {
        return this.id;
    }

    @Override
    public String name() throws JdbdException {
        return this.name;
    }


    @Override
    public int hashCode() {
        return Objects.hash(this.id, this.name);
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (obj == this) {
            match = true;
        } else if (obj instanceof JdbdSavePoint) {
            final JdbdSavePoint o = (JdbdSavePoint) obj;
            match = o.id == this.id && o.name.equals(this.name);
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public String toString() {
        return JdbdStrings.builder()
                .append(JdbdSavePoint.class.getName())
                .append("[ id : ")
                .append(this.id)
                .append(" , name : ")
                .append(this.name)
                .append(" , hash : ")
                .append(System.identityHashCode(this))
                .append(" ]")
                .toString();
    }


}