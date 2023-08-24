package io.jdbd.mysql.protocol;

import io.jdbd.JdbdException;
import io.jdbd.session.Option;
import io.jdbd.session.ServerVersion;

import java.util.Locale;
import java.util.Objects;

/**
 * <p>
 * This class is a implementation of {@link ServerVersion} with MySQL.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html">Protocol::HandshakeV10</a>
 * @since 1.0
 */
public final class MySQLServerVersion implements Comparable<MySQLServerVersion>, ServerVersion {

    private static final MySQLServerVersion MIN_VERSION = new MySQLServerVersion("0.0.0", 0, 0, 0);

    private final String completeVersion;
    private final int major;
    private final int minor;
    private final int subMinor;

    private MySQLServerVersion(String completeVersion, int major, int minor, int subMinor) {
        this.completeVersion = completeVersion;
        this.major = major;
        this.minor = minor;
        this.subMinor = subMinor;
    }

    public int compareTo(MySQLServerVersion other) {
        return doCompareTo(other.major, other.minor, other.subMinor);
    }


    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }

    /**
     * <p>
     * Beginning with MySQL 8.0.19, you can specify a time zone offset when inserting TIMESTAMP and DATETIME values into a table.
     * Datetime literals that include time zone offsets are accepted as parameter values by prepared statements.
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/date-and-time-literals.html">Date and Time Literals</a>
     */
    public boolean isSupportZoneOffset() {
        return this.meetsMinimum(8, 0, 19);
    }

    public boolean isSupportQueryAttr() {
        // Servers between 8.0.23 8.0.25 are affected by Bug#103102, Bug#103268 and Bug#103377. Query attributes cannot be sent to these servers.
        return this.meetsMinimum(8, 0, 26);
    }

    public boolean isSupportOutParameter() {
        return this.meetsMinimum(5, 5, 3);
    }


    @Override
    public String toString() {
        return this.completeVersion;
    }

    @Override
    public boolean equals(final Object obj) {
        final boolean match;
        if (this == obj) {
            match = true;
        } else if (obj instanceof MySQLServerVersion) {
            final MySQLServerVersion another = (MySQLServerVersion) obj;
            match = this.major == another.major
                    && this.minor == another.minor
                    && this.subMinor == another.subMinor;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.major, this.minor, this.subMinor);
    }

    /**
     * Does this version meet the minimum specified by `min'?
     *
     * @param min The minimum version to compare against.
     * @return true if version meets the minimum specified by `min'
     */
    public boolean meetsMinimum(MySQLServerVersion min) {
        return doCompareTo(min.major, min.minor, min.subMinor) >= 0;
    }

    public boolean meetsMinimum(int major, int minor, int subMinor) {
        return doCompareTo(major, minor, subMinor) >= 0;
    }

    @Override
    public String getVersion() {
        return this.completeVersion;
    }

    public int getMajor() {
        return this.major;
    }

    public int getMinor() {
        return this.minor;
    }

    public int getSubMinor() {
        return this.subMinor;
    }

    private int doCompareTo(int major, int minor, int subMinor) {
        int c;
        if ((c = Integer.compare(this.major, major)) != 0) {
            return c;
        } else if ((c = Integer.compare(this.minor, minor)) != 0) {
            return c;
        }
        return Integer.compare(this.subMinor, subMinor);
    }


    /**
     * Parse the server version into major/minor/subminor.
     *
     * @param versionString string version representation
     * @return {@link MySQLServerVersion}
     */
    public static MySQLServerVersion from(final String versionString) throws JdbdException {

        MySQLServerVersion serverVersion = null;

        try {
            final int major, minor, point1, pont2;
            point1 = versionString.indexOf('.');
            if (point1 < 0) {
                throw serverVersionError(versionString);
            }
            major = Integer.parseInt(versionString.substring(0, point1));
            pont2 = versionString.indexOf('.', point1 + 1);
            if (pont2 < 0) {
                throw serverVersionError(versionString);
            }
            minor = Integer.parseInt(versionString.substring(point1 + 1, pont2));

            final int len = versionString.length();

            char ch;
            for (int i = pont2 + 1, subMinor; i < len; i++) {
                ch = versionString.charAt(i);
                if (ch >= '0' && ch <= '9') {
                    continue;
                }
                subMinor = Integer.parseInt(versionString.substring(pont2 + 1, i));
                serverVersion = new MySQLServerVersion(versionString, major, minor, subMinor);
                break;
            }
            if (serverVersion == null) {
                // can't parse the server version
                serverVersion = MIN_VERSION;
            }
        } catch (Throwable e) {
            // can't parse the server version
            serverVersion = MIN_VERSION;
        }
        return serverVersion;
    }


    public static MySQLServerVersion getMinVersion() {
        return MIN_VERSION;
    }

    public static MySQLServerVersion getInstance(int major, int minor, int subMinor) {
        return new MySQLServerVersion(major + "." + minor + "." + subMinor, major, minor, subMinor);
    }

    public static boolean isEnterpriseEdition(MySQLServerVersion serverVersion) {
        final String completeVersion;
        completeVersion = serverVersion.completeVersion.toLowerCase(Locale.ROOT);
        return completeVersion.contains("enterprise")
                || completeVersion.contains("commercial")
                || completeVersion.contains("advanced");
    }

    private static JdbdException serverVersionError(String versionString) {
        return new JdbdException(String.format("versionString %s error", versionString));
    }

}
