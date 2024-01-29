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

package io.jdbd.mysql.protocol;

import io.jdbd.JdbdException;
import io.jdbd.session.Option;
import io.jdbd.session.ServerVersion;

import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * <p>
 * This class is a implementation of {@link ServerVersion} with MySQL.
 * <br/>
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

    @Override
    public <T> T valueOf(Option<T> option) {
        return null;
    }

    @Override
    public Set<Option<?>> optionSet() {
        return Collections.emptySet();
    }

    /**
     * <p>
     * Beginning with MySQL 8.0.19, you can specify a time zone offset when inserting TIMESTAMP and DATETIME values into a table.
     * Datetime literals that include time zone offsets are accepted as parameter values by prepared statements.
     * <br/>
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
            final MySQLServerVersion o = (MySQLServerVersion) obj;
            match = o.completeVersion.equals(this.completeVersion)
                    && this.major == o.major
                    && this.minor == o.minor
                    && this.subMinor == o.subMinor;
        } else {
            match = false;
        }
        return match;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.completeVersion, this.major, this.minor, this.subMinor);
    }

    /**
     * Does this version meet the minimum specified by `min'?
     *
     * @param min The minimum version to compare against.
     * @return true if version meets the minimum specified by `min'
     */
    public boolean meetsMinimum(MySQLServerVersion min) {
        return meetsMinimum(min.major, min.minor, min.subMinor);
    }

    @Override
    public boolean meetsMinimum(final int major, final int minor, final int subMinor) {
        final boolean meets;
        if (this.major != major) {
            meets = this.major >= major;
        } else if (this.minor != minor) {
            meets = this.minor >= minor;
        } else {
            meets = this.subMinor >= subMinor;
        }
        return meets;
    }

    @Override
    public int compareTo(final MySQLServerVersion o) {
        final int result;
        if (this.major != o.major) {
            result = this.major - o.major;
        } else if (this.minor != o.minor) {
            result = this.minor - o.minor;
        } else {
            result = this.subMinor - o.subMinor;
        }
        return result;
    }

    @Override
    public String getVersion() {
        return this.completeVersion;
    }

    @Override
    public int getMajor() {
        return this.major;
    }

    @Override
    public int getMinor() {
        return this.minor;
    }

    @Override
    public int getSubMinor() {
        return this.subMinor;
    }


    /**
     * Parse the server version into major/minor/subminor.
     *
     * @param versionString string version representation
     * @return {@link MySQLServerVersion}
     */
    public static MySQLServerVersion from(final String versionString) throws JdbdException {

        MySQLServerVersion serverVersion;

        try {
            final int major, minor, subMinor, point1, pont2;
            point1 = versionString.indexOf('.');
            major = Integer.parseInt(versionString.substring(0, point1));
            pont2 = versionString.indexOf('.', point1 + 1);
            minor = Integer.parseInt(versionString.substring(point1 + 1, pont2));

            final int len = versionString.length();

            int nonNumIndex = pont2 + 1;
            char ch;
            for (; nonNumIndex < len; nonNumIndex++) {
                ch = versionString.charAt(nonNumIndex);
                if (ch < '0' || ch > '9') {
                    break;
                }
            }
            subMinor = Integer.parseInt(versionString.substring(pont2 + 1, nonNumIndex));
            serverVersion = new MySQLServerVersion(versionString, major, minor, subMinor);
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

}
