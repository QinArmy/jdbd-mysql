package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.util.MySQLStrings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class MySQLNativePasswordPlugin implements AuthenticationPlugin {


    public static MySQLNativePasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {
        return new MySQLNativePasswordPlugin(protocolAssistant);
    }

    public static final String PLUGIN_NAME = "mysql_native_password";


    private final AuthenticateAssistant assistant;

    private MySQLNativePasswordPlugin(AuthenticateAssistant assistant) {
        this.assistant = assistant;
    }

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public boolean requiresConfidentiality() {
        return false;
    }

    @Override
    public ByteBuf nextAuthenticationStep(final ByteBuf fromServer) {
        final String password;
        password = this.assistant.getHostInfo().password();

        final ByteBuf payload;
        if (MySQLStrings.hasText(password)) {
            final byte[] passwordBytes, seed, scrambleBytes;
            passwordBytes = password.getBytes(this.assistant.getPasswordCharset());
            seed = Packets.readStringTermBytes(fromServer);
            scrambleBytes = AuthenticateUtils.scramble411(passwordBytes, seed);

            payload = this.assistant.allocator().buffer(scrambleBytes.length);
            payload.writeBytes(scrambleBytes);
        } else {
            payload = Unpooled.EMPTY_BUFFER;
        }
        return payload;
    }


}
