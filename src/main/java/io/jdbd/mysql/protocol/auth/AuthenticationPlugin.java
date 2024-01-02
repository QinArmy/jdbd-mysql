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

package io.jdbd.mysql.protocol.auth;

import io.netty.buffer.ByteBuf;

public interface AuthenticationPlugin {

    default void reset() {

    }


    /**
     * Returns the name that the MySQL server uses on
     * the wire for this plugin
     *
     * @return plugin name
     */
    String pluginName();

    /**
     * Does this plugin require the connection itself to be confidential
     * (i.e. tls/ssl)...Highly recommended to return "true" for plugins
     * that return the credentials in the clear.
     *
     * @return true if secure connection is required
     */
    boolean requiresConfidentiality();


    /**
     * Process authentication handshake data from server and optionally produce data to be sent back to the server.
     * The driver will keep calling this method on each new server packet arrival until either an Exception is thrown
     * (authentication failure, please use appropriate SQLStates) or the number of exchange iterations exceeded max
     * limit or an OK packet is sent by server indicating that the connection has been approved.
     * <p>
     * If, on return from this method, toServer is a non-empty list of buffers, then these buffers will be sent to
     * the server in the same order and without any reads in between them. If toServer is an empty list, no
     * data will be sent to server, driver immediately reads the next packet from server.
     * <p>
     * In case of errors the method should throw Exception.
     *
     * @param fromServer a buffer( reserved header) containing handshake data payload from
     *                   server (can be empty).
     *                   should contain data).
     * @return a unmodifiable list,that is list of payload,element is read-only, empty :authentication finish.
     */

    ByteBuf nextAuthenticationStep(ByteBuf fromServer);

}
