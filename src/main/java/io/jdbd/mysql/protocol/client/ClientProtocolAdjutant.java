package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.env.MySQLHost;
import io.netty.buffer.ByteBufAllocator;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.Map;

interface ClientProtocolAdjutant extends ResultRowAdjutant {


    Charset charsetClient();

    @Nullable
    Charset getCharsetResults();

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-errors.html">Error Message Character Set</a>
     */
    Charset errorCharset();

    Charset obtainCharsetMeta();

    /**
     * @return negotiated capability.
     */
    int capability();

    Map<Integer, Charsets.CustomCollation> obtainCustomCollationMap();


    Handshake10 handshake10();

    ByteBufAllocator allocator();

    MySQLHost host();


    SessionEnv sessionEnv();

}
