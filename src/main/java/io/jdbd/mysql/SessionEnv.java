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

    ZoneOffset serverZone();

    boolean isSupportLocalInfile();

}
