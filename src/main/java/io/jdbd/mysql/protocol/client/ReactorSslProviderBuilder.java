package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.MySQLHost;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.protocol.X509TrustManagerWrapper;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLStates;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.Environment;
import io.jdbd.vendor.util.Pair;
import io.jdbd.vendor.util.SQLStates;
import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.*;
import reactor.util.annotation.Nullable;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @see SslHandler
 */
final class ReactorSslProviderBuilder {


    static ReactorSslProviderBuilder builder(AuthenticateAssistant assistant) {
        return new ReactorSslProviderBuilder(assistant);
    }

    private final MySQLHost host;

    private final MySQLServerVersion serverVersion;

    private final ByteBufAllocator allocator;

    private final Environment env;

    private ReactorSslProviderBuilder(AuthenticateAssistant assistant) {
        this.host = assistant.getHostInfo();
        this.serverVersion = assistant.getServerVersion();
        this.allocator = assistant.allocator();
        this.env = this.host.properties();
    }


//    public reactor.netty.tcp.SslProvider build() throws JdbdException {
//
//        return reactor.netty.tcp.SslProvider.builder()
//                .sslContext(buildSslContext())
//                .build();
//    }

    public SslHandler buildSslHandler() throws JdbdException {
        final MySQLHost host = this.host;
        return buildSslContext().newHandler(this.allocator, host.host(), host.port());
    }

    public SslContext buildSslContext() throws JdbdException {


        final SslContextBuilder builder;
        builder = SslContextBuilder.forClient();
        // 1. config TrustManager
        configTrustManager(builder);
        //2. config KeyManager
        KeyManagerFactory keyManagerFactory = tryObtainKeyManagerFactory();
        if (keyManagerFactory != null) {
            builder.keyManager(keyManagerFactory);
        }
        //3. config ssl protocol.
        builder.protocols(obtainAllowedTlsProtocolList())
                // 4. config cipher suit.
                .ciphers(obtainAllowedCipherSuitList(), SupportedCipherSuiteFilter.INSTANCE);
        // 5. config provider
        if (OpenSsl.isAvailable()) {
            builder.sslProvider(SslProvider.OPENSSL);
        } else {
            builder.sslProvider(SslProvider.JDK);
        }
        try {
            return builder.build();
        } catch (SSLException e) {
            String message = String.format("Cannot create %s due to [%s]", SslHandler.class.getName(), e.getMessage());
            throw new JdbdException(message, MySQLStates.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, 0, e);
        }
    }


    private List<String> obtainAllowedCipherSuitList() {
        List<String> candidateList;
        candidateList = MySQLStrings.spitAsList(this.env.get(MySQLKey.ENABLED_SSL_CIPHER_SUITES), ",", false);

        if (candidateList.isEmpty()) {
            candidateList = SslUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST;
        } else {
            candidateList.retainAll(SslUtils.CLIENT_SUPPORT_TLS_CIPHER_LIST);
        }

        List<String> allowedCipherList = MySQLCollections.arrayList(candidateList.size());
        candidateFor:
        for (String candidate : candidateList) {
            for (String restricted : SslUtils.MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST) {
                if (candidate.contains(restricted)) {
                    continue candidateFor;
                }
            }
            allowedCipherList.add(candidate);
        }
        return Collections.unmodifiableList(allowedCipherList);
    }

    /**
     * @return a unmodifiable list
     */
    private List<String> obtainAllowedTlsProtocolList() {
        List<String> candidateList;
        candidateList = MySQLStrings.spitAsList(this.env.get(MySQLKey.ENABLED_TLS_PROTOCOLS), ",", false);

        if (candidateList.isEmpty()) {
            MySQLServerVersion serverVersion = this.serverVersion;

            if (serverVersion.meetsMinimum(5, 7, 28)
                    || (serverVersion.meetsMinimum(5, 6, 46) && !serverVersion.meetsMinimum(5, 7, 0))
                    || (serverVersion.meetsMinimum(5, 6, 0) && MySQLServerVersion.isEnterpriseEdition(serverVersion))) {
                candidateList = SslUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST;
            } else {
                candidateList = Collections.unmodifiableList(Arrays.asList(SslUtils.TLSv1_1, SslUtils.TLSv1));
            }
        } else {
            List<String> supportProtocolList = MySQLCollections.arrayList(SslUtils.CLIENT_SUPPORT_TLS_PROTOCOL_LIST);
            supportProtocolList.retainAll(candidateList);
            candidateList = Collections.unmodifiableList(supportProtocolList);
        }
        return candidateList;
    }


    private void configTrustManager(final io.netty.handler.ssl.SslContextBuilder builder) throws JdbdException {
        final Enums.SslMode sslMode;
        sslMode = this.env.getOrDefault(MySQLKey.SSL_MODE);
        final boolean verify = sslMode == Enums.SslMode.VERIFY_CA
                || sslMode == Enums.SslMode.VERIFY_IDENTITY;
        try {

            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(false);

            if (storePair != null || verify) {
                TrustManagerFactory tmfWrapper = new TrustManagerFactoryWrapper(
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()),
                        verify, this.host.host());

                if (storePair == null) {
                    tmfWrapper.init((KeyStore) null); //initializes the TrustManagerFactory with the default truststore.
                } else {
                    tmfWrapper.init(storePair.getFirst());
                }
                builder.trustManager(tmfWrapper);
            } else {
                builder.trustManager(new X509TrustManagerWrapper(this.host.host()));

            }

        } catch (NoSuchAlgorithmException e) {
            String message = String.format("%s algorithm[%s] not found.", TrustManagerFactory.class.getName(),
                    TrustManagerFactory.getDefaultAlgorithm());
            throw new JdbdException(message, MySQLStates.CONNECTION_EXCEPTION, 0, e);
        } catch (KeyStoreException e) {
            String message = String.format("Cannot init %s due to %s", TrustManagerFactory.class.getName(),
                    e.getMessage());
            throw new JdbdException(message, MySQLStates.CONNECTION_EXCEPTION, 0, e);
        }
    }

    @Nullable
    private KeyManagerFactory tryObtainKeyManagerFactory() throws JdbdException {
        try {
            Pair<KeyStore, char[]> storePair = tryObtainKeyStorePasswordPairForSsl(true);
            if (storePair == null) {
                return null;
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(storePair.getFirst(), storePair.getSecond());
            return kmf;
        } catch (NoSuchAlgorithmException e) {
            String message = String.format("%s algorithm[%s] not found.", KeyManagerFactory.class.getName()
                    , KeyManagerFactory.getDefaultAlgorithm());
            throw new JdbdException(message, MySQLStates.CONNECTION_EXCEPTION, 0, e);
        } catch (KeyStoreException | UnrecoverableKeyException e) {
            String message = String.format("Cannot init %s due to %s", KeyManagerFactory.class.getName(),
                    e.getMessage());
            throw new JdbdException(message, MySQLStates.CONNECTION_EXCEPTION, 0, e);
        }
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">Security</a>
     */
    @Nullable
    private Pair<KeyStore, char[]> tryObtainKeyStorePasswordPairForSsl(final boolean key) throws JdbdException {
        // 1. below obtain three storeUrl,storeType,storePassword
        final MySQLKey<String> storeUrlKey, storeTypeKey, passwordKey;
        final String systemStoreUrlKey, systemStoreTypeKey, systemPasswordKey;
        if (key) {
            storeUrlKey = MySQLKey.CLIENT_CERTIFICATE_KEY_STORE_URL;
            storeTypeKey = MySQLKey.CLIENT_CERTIFICATE_KEY_STORE_TYPE;
            passwordKey = MySQLKey.CLIENT_CERTIFICATE_KEY_STORE_PASSWORD;

            systemStoreUrlKey = "javax.net.ssl.keyStore";
            systemStoreTypeKey = "javax.net.ssl.keyStoreType";
            systemPasswordKey = "javax.net.ssl.keyStorePassword";
        } else {
            storeUrlKey = MySQLKey.TRUST_CERTIFICATE_KEY_STORE_URL;
            storeTypeKey = MySQLKey.TRUST_CERTIFICATE_KEY_STORE_TYPE;
            passwordKey = MySQLKey.TRUST_CERTIFICATE_KEY_STORE_PASSWORD;

            systemStoreUrlKey = "javax.net.ssl.trustStore";
            systemStoreTypeKey = "javax.net.ssl.trustStoreType";
            systemPasswordKey = "javax.net.ssl.trustStorePassword";
        }

        final Environment env = this.env;
        String storeUrl, storeType, storePwd;

        storeUrl = env.get(storeUrlKey);
        storeType = env.get(storeTypeKey);
        storePwd = env.get(passwordKey);

        if (!MySQLStrings.hasText(storeUrl)) {
            boolean useSystem = (key && env.getOrDefault(MySQLKey.FALLBACK_TO_SYSTEM_KEY_STORE))
                    || (!key && env.getOrDefault(MySQLKey.FALLBACK_TO_SYSTEM_TRUST_STORE));
            if (useSystem) {
                storeUrl = System.getProperty(systemStoreUrlKey);
                storeType = System.getProperty(systemStoreTypeKey);
                storePwd = System.getProperty(systemPasswordKey);
            }
            if (!MySQLStrings.hasText(storeType)) {
                storeType = env.get(storeTypeKey);
            }

        }

        if (MySQLStrings.hasText(storeUrl)) {
            try {
                URI.create(storeUrl).toURL();
            } catch (MalformedURLException e) {
                storeUrl = "file:" + storeUrl;
            }
            if (!MySQLStrings.hasText(storeType)) {
                return null;
            }
        } else {
            return null;
        }

        final char[] storePassword = (storePwd == null) ? new char[0] : storePwd.toCharArray();
        // 2. create and init KeyStore with three storeUrl,storeType,storePassword
        try (InputStream storeInput = URI.create(storeUrl).toURL().openStream()) {
            KeyStore keyStore = KeyStore.getInstance(storeType);
            keyStore.load(storeInput, storePassword);
            return Pair.create(keyStore, storePassword);
        } catch (MalformedURLException e) {
            throw new JdbdException(String.format("%s[%s] isn't url.", storeUrlKey, storeUrl),
                    MySQLStates.CONNECTION_EXCEPTION, 0, e);
        } catch (KeyStoreException e) {
            String m = String.format("%s[%s] is KeyStore type than is supported by provider.", storeTypeKey, storeType);
            throw new JdbdException(m, MySQLStates.CONNECTION_EXCEPTION, 0, e);
        } catch (NoSuchAlgorithmException | IOException | CertificateException e) {
            String message = String.format("Cannot load KeyStore by url[%s] and type[%s]", storeUrl, storeType);
            throw new JdbdException(message, MySQLStates.CONNECTION_EXCEPTION, 0, e);
        }
    }


    private static final class TrustManagerFactoryWrapper extends TrustManagerFactory {

        private TrustManagerFactoryWrapper(TrustManagerFactory factory, boolean verify, String host) {
            super(new TrustManagerFactorySpiWrapper(factory, verify, host), factory.getProvider(),
                    factory.getAlgorithm());
        }


    }

    private static final class TrustManagerFactorySpiWrapper extends TrustManagerFactorySpi {

        private final TrustManagerFactory factory;

        private final boolean verify;

        private final String host;

        private TrustManagerFactorySpiWrapper(TrustManagerFactory factory, boolean verify, String host) {
            this.factory = factory;
            this.verify = verify;
            this.host = host;
        }

        @Override
        protected void engineInit(KeyStore keyStore) throws KeyStoreException {
            this.factory.init(keyStore);
        }

        @Override
        protected void engineInit(ManagerFactoryParameters managerFactoryParameters)
                throws InvalidAlgorithmParameterException {
            this.factory.init(managerFactoryParameters);
        }

        @Override
        protected TrustManager[] engineGetTrustManagers() {
            TrustManager[] trustManagerArray = this.factory.getTrustManagers();
            TrustManager[] wrapperArray = new TrustManager[trustManagerArray.length];

            try {
                for (int i = 0; i < trustManagerArray.length; i++) {
                    TrustManager tm = trustManagerArray[i];

                    if (tm instanceof X509TrustManager) {
                        wrapperArray[i] = new X509TrustManagerWrapper((X509TrustManager) tm, this.verify, this.host);
                    } else {
                        wrapperArray[i] = tm;
                    }

                }
            } catch (Throwable e) {
                String message = String.format("Can't create %s due to %s.", X509TrustManagerWrapper.class.getName(),
                        e.getMessage());
                throw new JdbdException(message, SQLStates.CONNECTION_EXCEPTION, 0, e);
            }
            return wrapperArray;
        }

    }


}
