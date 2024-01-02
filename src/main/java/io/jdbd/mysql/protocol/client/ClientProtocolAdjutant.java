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

package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.SessionEnv;
import io.jdbd.mysql.env.MySQLHostInfo;
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

    Map<Integer, CustomCollation> obtainCustomCollationMap();


    Handshake10 handshake10();

    ByteBufAllocator allocator();

    MySQLHostInfo host();


    SessionEnv sessionEnv();

}
