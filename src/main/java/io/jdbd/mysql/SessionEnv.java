package io.jdbd.mysql;

import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

public interface SessionEnv {

    boolean containSqlMode(SQLMode sqlMode);

    Charset charsetClient();

    @Nullable
    Charset charsetResults();

    @Nullable
    ZoneOffset connZone();

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables">Time Zone Variables</a>
     */
    ZoneOffset serverZone();

}
