package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.AuthenticateAssistant;
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
