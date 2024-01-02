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

import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.client.Packets;
import io.netty.buffer.ByteBuf;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class MySQLClearPasswordPlugin implements AuthenticationPlugin {

    public static MySQLClearPasswordPlugin getInstance(AuthenticateAssistant assistant) {
        return new MySQLClearPasswordPlugin(assistant);
    }

    public static final String PLUGIN_NAME = "mysql_clear_password";


    private final AuthenticateAssistant assistant;

    private MySQLClearPasswordPlugin(AuthenticateAssistant assistant) {
        this.assistant = assistant;
    }

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return true;
    }

    @Override
    public ByteBuf nextAuthenticationStep(final ByteBuf fromServer) {

        final AuthenticateAssistant assistant = this.assistant;

        final Charset passwordCharset;
        if (assistant.getServerVersion().meetsMinimum(5, 7, 6)) {
            passwordCharset = assistant.getPasswordCharset();
        } else {
            passwordCharset = StandardCharsets.UTF_8;
        }

        final String password;
        password = assistant.getHostInfo().password();
        byte[] passwordBytes;
        if (password == null) {
            passwordBytes = "".getBytes(passwordCharset);
        } else {
            passwordBytes = password.getBytes(passwordCharset);
        }
        final ByteBuf payload;
        payload = assistant.allocator().buffer(passwordBytes.length + 1);
        Packets.writeStringTerm(payload, passwordBytes);
        return payload;
    }


}
