package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.security.DigestException;

/**
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html">Caching_sha2_password</a>
 */
public class CachingSha2PasswordPlugin extends Sha256PasswordPlugin {


    public static CachingSha2PasswordPlugin getInstance(AuthenticateAssistant protocolAssistant) {
        return new CachingSha2PasswordPlugin(protocolAssistant, tryLoadPublicKeyString(protocolAssistant.getHostInfo()));
    }

    public static final String PLUGIN_NAME = "caching_sha2_password";

    private static final Logger LOG = LoggerFactory.getLogger(CachingSha2PasswordPlugin.class);


    protected AuthStage stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;

    protected CachingSha2PasswordPlugin(AuthenticateAssistant protocolAssistant, @Nullable String publicKeyString) {
        super(protocolAssistant, publicKeyString);
    }

    @Override
    public void reset() {
        super.reset();
        this.stage = AuthStage.FAST_AUTH_SEND_SCRAMBLE;
    }

    @Override
    public String pluginName() {
        return PLUGIN_NAME;
    }


    @Override
    protected ByteBuf internalNextAuthenticationStep(String password, ByteBuf fromServer) {
        final AuthStage stage = this.stage;

        try {
            if (stage == AuthStage.FAST_AUTH_SEND_SCRAMBLE) {
                // send a scramble for fast auth
                String seedString = Packets.readStringTerm(fromServer, Charset.defaultCharset());
                this.seed = seedString;

                byte[] passwordBytes = password.getBytes(this.assistant.getPasswordCharset());
                byte[] sha2Bytes = AuthenticateUtils.scrambleCachingSha2(passwordBytes, seedString.getBytes());
                ByteBuf payloadBuffer = this.assistant.allocator().buffer(sha2Bytes.length);
                payloadBuffer.writeBytes(sha2Bytes);

                this.stage = AuthStage.FAST_AUTH_READ_RESULT;
                LOG.trace("use fast auth send scramble.");
                return payloadBuffer.asReadOnly();
            } else if (stage == AuthStage.FAST_AUTH_READ_RESULT) {
                int flag = Packets.readInt1AsInt(fromServer);
                switch (flag) {
                    case 3:
                        this.stage = AuthStage.FAST_AUTH_COMPLETE;
                        return Unpooled.EMPTY_BUFFER;
                    case 4:
                        this.stage = AuthStage.FULL_AUTH;
                        LOG.debug("Server demand FULL_AUTH");
                        break;
                    default:
                        throw new JdbdException(String.format("Unknown server response[%s] after fast auth.", flag));
                }
            }
        } catch (DigestException e) {
            throw new JdbdException("password encrypt failure.", e);
        }

        return doNextAuthenticationStep(password, fromServer);
    }

    @Override
    protected int getPublicKeyRetrievalPacketFlag() {
        return 2;
    }

    @Override
    protected byte[] encryptPassword(String password) {
        return this.assistant.getServerVersion().meetsMinimum(8, 0, 5)
                ? super.encryptPassword(password)
                : super.encryptPassword(password, "RSA/ECB/PKCS1Padding");
    }

    /*################################## blow static inner class ##################################*/


    public enum AuthStage {
        FAST_AUTH_SEND_SCRAMBLE,
        FAST_AUTH_READ_RESULT,
        FAST_AUTH_COMPLETE,
        FULL_AUTH
    }

}
