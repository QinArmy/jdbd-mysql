package io.jdbd.mysql.protocol;

import io.jdbd.session.ServerVersion;

import java.util.Objects;

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
    public static MySQLServerVersion from(final String versionString) {
        final int index1 = versionString.indexOf('.');
        if (index1 < 0) {
            throw new IllegalArgumentException("versionString error");
        }
        int major = Integer.parseInt(versionString.substring(0, index1));
        final int index2 = versionString.indexOf('.', index1 + 1);
        if (index2 < 0) {
            throw new IllegalArgumentException("versionString error");
        }
        int minor = Integer.parseInt(versionString.substring(index1 + 1, index2));

        final int len = versionString.length();
        for (int i = index2 + 1; i < len; i++) {
            if ((versionString.charAt(i) < '0') || (versionString.charAt(i) > '9')) {
                continue;
            }
            int subMinor = Integer.parseInt(versionString.substring(i));
            return new MySQLServerVersion(versionString, major, minor, subMinor);
        }
        throw new IllegalArgumentException("versionString error");
    }

    public static MySQLServerVersion getMinVersion() {
        return MIN_VERSION;
    }

    public static MySQLServerVersion getInstance(int major, int minor, int subMinor) {
        return new MySQLServerVersion(major + "." + minor + "." + subMinor, major, minor, subMinor);
    }

    public static boolean isEnterpriseEdition(MySQLServerVersion serverVersion) {
        String completeVersion = serverVersion.completeVersion;
        return completeVersion.contains("enterprise")
                || completeVersion.contains("commercial")
                || completeVersion.contains("advanced");
    }

}
