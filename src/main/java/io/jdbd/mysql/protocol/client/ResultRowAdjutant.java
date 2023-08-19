package io.jdbd.mysql.protocol.client;


import io.jdbd.lang.Nullable;

import java.nio.charset.Charset;
import java.time.ZoneOffset;

interface ResultRowAdjutant {

    ZoneOffset connZone();

    @Nullable
    ZoneOffset serverZone();

    @Nullable
    Charset getCharsetResults();

    Charset columnCharset(Charset columnCharset);


}
