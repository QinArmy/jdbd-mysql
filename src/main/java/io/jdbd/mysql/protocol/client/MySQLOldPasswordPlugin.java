package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.util.MySQLStrings;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

/**
 * <p>
 * see  {@code com.mysql.cj.protocol.a.authentication.MysqlOldPasswordPlugin}
 * </p>
 */
public class MySQLOldPasswordPlugin implements AuthenticationPlugin {

    public static MySQLOldPasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {
        return new MySQLOldPasswordPlugin(protocolAssistant);
    }

    public static final String PLUGIN_NAME = "mysql_old_password";

    private final AuthenticateAssistant assistant;


    private MySQLOldPasswordPlugin(AuthenticateAssistant assistant) {
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
        final AuthenticateAssistant assistant = this.assistant;

        final String password;
        password = assistant.getHostInfo().password();
        final ByteBuf payload;
        if (MySQLStrings.hasText(password)) {
            final String seed, cryptString;
            seed = Packets.readStringTerm(fromServer, Charset.defaultCharset()).substring(0, 8);
            cryptString = newCrypt(password, seed, this.assistant.getPasswordCharset());
            final byte[] payloadBytes;
            payloadBytes = cryptString.getBytes();

            payload = this.assistant.allocator().buffer(payloadBytes.length);
            payload.writeBytes(payloadBytes);

        } else {
            payload = Unpooled.EMPTY_BUFFER;
        }
        return payload;
    }

    // Right from Monty's code
    private static String newCrypt(String password, String seed, Charset encoding) {
        byte b;
        double d;

        long[] pw = newHash(seed.getBytes());
        long[] msg = hashPre41Password(password, encoding);
        long max = 0x3fffffffL;
        long seed1 = (pw[0] ^ msg[0]) % max;
        long seed2 = (pw[1] ^ msg[1]) % max;
        char[] chars = new char[seed.length()];

        for (int i = 0; i < seed.length(); i++) {
            seed1 = ((seed1 * 3) + seed2) % max;
            seed2 = (seed1 + seed2 + 33) % max;
            d = (double) seed1 / (double) max;
            b = (byte) java.lang.Math.floor((d * 31) + 64);
            chars[i] = (char) b;
        }

        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = (double) seed1 / (double) max;
        b = (byte) java.lang.Math.floor(d * 31);

        for (int i = 0; i < seed.length(); i++) {
            chars[i] ^= (char) b;
        }

        return new String(chars);
    }

    private static long[] hashPre41Password(String password, Charset encoding) {
        // remove white spaces and convert to bytes
        return newHash(password.replaceAll("\\s", "").getBytes(encoding));
    }

    private static long[] newHash(byte[] password) {
        long nr = 1345345333L;
        long add = 7;
        long nr2 = 0x12345671L;
        long tmp;

        for (byte b : password) {
            tmp = 0xff & b;
            nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
            nr2 += ((nr2 << 8) ^ nr);
            add += tmp;
        }

        long[] result = new long[2];
        result[0] = nr & 0x7fffffffL;
        result[1] = nr2 & 0x7fffffffL;

        return result;
    }
}
