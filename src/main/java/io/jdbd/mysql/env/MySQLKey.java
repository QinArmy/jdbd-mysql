package io.jdbd.mysql.env;

import io.jdbd.Driver;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.protocol.client.Enums;
import io.jdbd.mysql.protocol.client.MySQLNativePasswordPlugin;
import io.jdbd.vendor.env.Key;
import io.jdbd.vendor.env.Redefine;
import reactor.netty.resources.ConnectionProvider;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public final class MySQLKey<T> extends Key<T> {

    /*
     * Properties individually managed after parsing connection string. These property keys are case insensitive.
     */

    /*-------------------below Authentication group-------------------*/


    /**
     * The database user name.
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">user</a>
     */
    public static final MySQLKey<String> USER = new MySQLKey<>(Driver.USER, String.class, null);

    /**
     * The database user password.
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password</a>
     */
    public static final MySQLKey<String> PASSWORD = new MySQLKey<>(Driver.PASSWORD, String.class, null);

    /**
     * password1
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password1</a>
     * @since MySQL Driver  8.0.28
     */
    public static final MySQLKey<String> PASSWORD1 = new MySQLKey<>("password1", String.class, null);

    /**
     * password2
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password2</a>
     * @since MySQL Driver  8.0.28
     */
    public static final MySQLKey<String> PASSWORD2 = new MySQLKey<>("password2", String.class, null);

    /**
     * password3
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">password3</a>
     * @since MySQL Driver  8.0.28
     */
    public static final MySQLKey<String> PASSWORD3 = new MySQLKey<>("password3", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">authenticationPlugins</a>
     */
    public static final MySQLKey<String> AUTHENTICATION_PLUGINS = new MySQLKey<>("authenticationPlugins", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">defaultAuthenticationPlugin</a>
     */
    public static final MySQLKey<String> DEFAULT_AUTHENTICATION_PLUGIN = new MySQLKey<>("defaultAuthenticationPlugin", String.class, MySQLNativePasswordPlugin.PLUGIN_NAME);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">disabledAuthenticationPlugins</a>
     */
    public static final MySQLKey<String> DISABLED_AUTHENTICATION_PLUGINS = new MySQLKey<>("disabledAuthenticationPlugins", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">ociConfigFile</a>
     * @since MySQL Driver  8.0.27
     */
    public static final MySQLKey<String> OCI_CONFIG_FILE = new MySQLKey<>("ociConfigFile", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">ociConfigProfile</a>
     * @since MySQL Driver  8.0.33
     */
    public static final MySQLKey<String> OCI_CONFIG_PROFILE = new MySQLKey<>("ociConfigProfile", String.class, "DEFAULT");

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">authenticationFidoCallbackHandler</a>
     * @since MySQL Driver  8.0.29
     */
    @Deprecated
    public static final MySQLKey<String> AUTHENTICATION_FIDO_CALLBACK_HANDLER = new MySQLKey<>("authenticationFidoCallbackHandler", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-authentication.html">ldapServerHostname</a>
     * @since MySQL Driver  8.0.23
     */
    public static final MySQLKey<String> LDAP_SERVER_HOSTNAME = new MySQLKey<>("ldapServerHostname", String.class, null);

    /*-------------------below Connection group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">connectionAttributes</a>
     */
    public static final MySQLKey<String> CONNECTION_ATTRIBUTES = new MySQLKey<>("connectionAttributes", String.class, null);


    // TODO connectionLifecycleInterceptors


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">useConfigs</a>
     */
    public static final MySQLKey<String> USE_CONFIGS = new MySQLKey<>("useConfigs", String.class, null);

    /**
     * <p>
     * {@link java.util.function.Supplier} function reference. For example : MyClass::clientInoMap
     * <pre><br/>
     *   public static Map&lt;String,String> clientInoMap() {
     *       return Collections.emptyMap();
     *   }
     * </pre>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">clientInfoSupplier</a>
     */
    public static final MySQLKey<String> CLIENT_INFO_SUPPLIER = new MySQLKey<>("clientInfoSupplier", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">createDatabaseIfNotExist</a>
     */
    public static final MySQLKey<Boolean> CREATE_DATABASE_IF_NOT_EXIST = new MySQLKey<>("createDatabaseIfNotExist", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">detectCustomCollations</a>
     */
    public static final MySQLKey<Boolean> DETECT_CUSTOM_COLLATIONS = new MySQLKey<>("detectCustomCollations", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">disconnectOnExpiredPasswords</a>
     */
    public static final MySQLKey<Boolean> DISCONNECT_ON_EXPIRED_PASSWORDS = new MySQLKey<>("disconnectOnExpiredPasswords", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">interactiveClient</a>
     */
    public static final MySQLKey<Boolean> INTERACTIVE_CLIENT = new MySQLKey<>("interactiveClient", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">passwordCharacterEncoding</a>
     */
    public static final MySQLKey<Charset> PASSWORD_CHARACTER_ENCODING = new MySQLKey<>("passwordCharacterEncoding", Charset.class, null);

    /**
     * <p>
     * {@link java.util.function.UnaryOperator} function reference. For example : MyClass::transformProperties
     * <pre><br/>
     *   public static Map&lt;String,String> transformProperties(Map&lt;String,String> map) {
     *       return map;
     *   }
     * </pre>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">propertiesTransform</a>
     */
    public static final MySQLKey<String> PROPERTIES_TRANS_FORM = new MySQLKey<>("propertiesTransform", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">rollbackOnPooledClose</a>
     */
    public static final MySQLKey<Boolean> ROLLBACK_ON_POOLED_CLOSE = new MySQLKey<>("rollbackOnPooledClose", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-connection.html">useAffectedRows</a>
     */
    public static final MySQLKey<Boolean> USE_AFFECTED_ROWS = new MySQLKey<>("useAffectedRows", Boolean.class, Boolean.FALSE);

    /*-------------------below Session group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">sessionVariables</a>
     */
    public static final MySQLKey<String> SESSION_VARIABLES = new MySQLKey<>("sessionVariables", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">characterEncoding</a>
     */
    public static final MySQLKey<Charset> CHARACTER_ENCODING = new MySQLKey<>("characterEncoding", Charset.class, StandardCharsets.UTF_8);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">characterSetResults</a>
     */
    public static final MySQLKey<String> CHARACTER_SET_RESULTS = new MySQLKey<>("characterSetResults", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">connectionCollation</a>
     */
    public static final MySQLKey<String> CONNECTION_COLLATION = new MySQLKey<>("connectionCollation", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">customCharsetMapping</a>
     * @since MySQL Driver  8.0.26
     */
    public static final MySQLKey<String> CUSTOM_CHARSET_MAPPING = new MySQLKey<>("customCharsetMapping", String.class, null);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">trackSessionState</a>
//     * @since MySQL Driver  8.0.26
//     * @deprecated jdbd-mysql always support for better receive and send message.
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> TRACK_SESSION_STATE = new MySQLKey<>("trackSessionState", Boolean.class, Boolean.FALSE);


    /*-------------------below Networking group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">socksProxyHost</a>
     */
    public static final MySQLKey<String> SOCKS_PROXY_HOST = new MySQLKey<>("socksProxyHost", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">socksProxyPort</a>
     */
    public static final MySQLKey<Integer> SOCKS_PROXY_PORT = new MySQLKey<>("socksProxyPort", Integer.class, 1080);

    /**
     * <p>
     * The class name that must is the implementation of {@link ConnectionProvider}.
     * The class must provide public static factory getInstance().For example :
     * <pre><br/>
     *   public static ConnectionProvider getInstance() {
     *       return instance;
     *   }
     * </pre>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">socketFactory</a>
     */
    @Redefine
    public static final MySQLKey<ConnectionProvider> SOCKET_FACTORY = new MySQLKey<>("socketFactory", ConnectionProvider.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">connectTimeout</a>
     */
    public static final MySQLKey<Long> CONNECT_TIMEOUT = new MySQLKey<>("connectTimeout", Long.class, 0L);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">socketTimeout</a>
     */
    public static final MySQLKey<Long> SOCKET_TIMEOUT = new MySQLKey<>("socketTimeout", Long.class, 0L);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">dnsSrv</a>
     * @since MySQL Driver  8.0.19
     */
    public static final MySQLKey<Boolean> DNS_SRV = new MySQLKey<>("dnsSrv", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">localSocketAddress</a>
     */
    public static final MySQLKey<String> LOCAL_SOCKET_ADDRESS = new MySQLKey<>("localSocketAddress", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">maxAllowedPacket</a>
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    public static final MySQLKey<Integer> MAX_ALLOWED_PACKET = new MySQLKey<>("maxAllowedPacket", Integer.class, 1 << 26); //67108864

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">socksProxyRemoteDns</a>
     * @since MySQL Driver  8.0.29
     */
    public static final MySQLKey<Boolean> SOCKS_PROXY_REMOTE_DNS = new MySQLKey<>("socksProxyRemoteDns", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">tcpKeepAlive</a>
     * @since MySQL Driver  5.0.7
     */
    public static final MySQLKey<Boolean> TCP_KEEP_ALIVE = new MySQLKey<>("tcpKeepAlive", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">tcpNoDelay</a>
     * @since MySQL Driver  5.0.7
     */
    public static final MySQLKey<Boolean> TCP_NO_DELAY = new MySQLKey<>("tcpNoDelay", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">tcpRcvBuf</a>
     * @since MySQL Driver  5.0.7
     */
    public static final MySQLKey<Integer> TCP_RCV_BUF = new MySQLKey<>("tcpRcvBuf", Integer.class, 0);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">tcpSndBuf</a>
     * @since MySQL Driver  5.0.7
     */
    public static final MySQLKey<Integer> TCP_SND_BUF = new MySQLKey<>("tcpSndBuf", Integer.class, 0);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">tcpTrafficClass</a>
     * @since MySQL Driver  5.0.7
     */
    public static final MySQLKey<Integer> TCP_TRAFFIC_CLASS = new MySQLKey<>("tcpTrafficClass", Integer.class, 0);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">useCompression</a>
     * @since MySQL Driver  3.0.17
     */
    public static final MySQLKey<Boolean> USE_COMPRESSION = new MySQLKey<>("useCompression", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-networking.html">useUnbufferedInput</a>
     * @since MySQL Driver  3.0.11
     */
    public static final MySQLKey<Boolean> USE_UNBUFFERED_INPUT = new MySQLKey<>("useUnbufferedInput", Boolean.class, Boolean.TRUE);

    /**
     * @since jdbd-mysql 1.0
     */
    public static final MySQLKey<String> ENABLED_SSL_CIPHER_SUITES = new MySQLKey<>("enabledSSLCipherSuites", String.class, null);

    /**
     * @since jdbd-mysql 1.0
     */
    public static final MySQLKey<String> ENABLED_TLS_PROTOCOLS = new MySQLKey<>("enabledTLSProtocols", String.class, null);


    /*-------------------below Security group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">paranoid</a>
     * @since MySQL Driver  3.0.1
     */
    public static final MySQLKey<Boolean> PARANOID = new MySQLKey<>("paranoid", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">serverRSAPublicKeyFile</a>
     * @since MySQL Driver  5.1.31
     */
    public static final MySQLKey<Path> SERVER_RSA_PUBLIC_KEY_FILE = new MySQLKey<>("serverRSAPublicKeyFile", Path.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">allowPublicKeyRetrieval</a>
     * @since MySQL Driver  5.1.31
     */
    public static final MySQLKey<Boolean> ALLOW_PUBLIC_KEY_RETRIEVAL = new MySQLKey<>("allowPublicKeyRetrieval", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">sslMode</a>
     * @since MySQL Driver  8.0.13
     */
    public static final MySQLKey<Enums.SslMode> SSL_MODE = new MySQLKey<>("sslMode", Enums.SslMode.class, Enums.SslMode.PREFERRED);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">trustCertificateKeyStoreUrl</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<String> TRUST_CERTIFICATE_KEY_STORE_URL = new MySQLKey<>("trustCertificateKeyStoreUrl", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">trustCertificateKeyStoreType</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<String> TRUST_CERTIFICATE_KEY_STORE_TYPE = new MySQLKey<>("trustCertificateKeyStoreType", String.class, "JKS");

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">trustCertificateKeyStorePassword</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<String> TRUST_CERTIFICATE_KEY_STORE_PASSWORD = new MySQLKey<>("trustCertificateKeyStorePassword", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">fallbackToSystemTrustStore</a>
     * @since MySQL Driver  8.0.22
     */
    public static final MySQLKey<Boolean> FALLBACK_TO_SYSTEM_TRUST_STORE = new MySQLKey<>("fallbackToSystemTrustStore", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">clientCertificateKeyStoreUrl</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<String> CLIENT_CERTIFICATE_KEY_STORE_URL = new MySQLKey<>("clientCertificateKeyStoreUrl", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">clientCertificateKeyStoreType</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<String> CLIENT_CERTIFICATE_KEY_STORE_TYPE = new MySQLKey<>("clientCertificateKeyStoreType", String.class, "JKS");

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">clientCertificateKeyStorePassword</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<String> CLIENT_CERTIFICATE_KEY_STORE_PASSWORD = new MySQLKey<>("clientCertificateKeyStorePassword", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">fallbackToSystemKeyStore</a>
     * @since MySQL Driver  8.0.22
     */
    public static final MySQLKey<Boolean> FALLBACK_TO_SYSTEM_KEY_STORE = new MySQLKey<>("fallbackToSystemKeyStore", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">tlsCiphersuites</a>
     * @since MySQL Driver  5.1.35
     */
    public static final MySQLKey<String> TLS_CIPHER_SUITES = new MySQLKey<>("tlsCiphersuites", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">tlsVersions</a>
     * @since MySQL Driver  8.0.8
     */
    public static final MySQLKey<String> TLS_VERSIONS = new MySQLKey<>("tlsVersions", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">allowLoadLocalInfile</a>
     * @since MySQL Driver  3.0.3
     */
    public static final MySQLKey<Boolean> ALLOW_LOAD_LOCAL_INFILE = new MySQLKey<>("allowLoadLocalInfile", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">allowLoadLocalInfileInPath</a>
     * @since MySQL Driver  8.0.8
     */
    public static final MySQLKey<Path> ALLOW_LOAD_LOCAL_INFILE_IN_PATH = new MySQLKey<>("allowLoadLocalInfileInPath", Path.class, null);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">allowMultiQueries</a>
//     * @since MySQL Driver  3.1.1
//     * @deprecated jdbd-mysql always support
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> ALLOW_MULTI_QUERIES = new MySQLKey<>("allowMultiQueries", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-security.html">allowUrlInLocalInfile</a>
     * @since MySQL Driver  3.1.4
     */
    public static final MySQLKey<Boolean> ALLOW_URL_IN_LOCAL_INFILE = new MySQLKey<>("allowUrlInLocalInfile", Boolean.class, Boolean.FALSE);


    /*-------------------below Statements group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-statements.html">cacheDefaultTimeZone</a>
     * @since MySQL Driver  8.0.20
     */
    public static final MySQLKey<Boolean> CACHE_DEFAULT_TIME_ZONE = new MySQLKey<>("cacheDefaultTimeZone", Boolean.class, Boolean.TRUE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-statements.html">continueBatchOnError</a>
     * @since MySQL Driver  3.0.3
     */
    public static final MySQLKey<Boolean> CONTINUE_BATCH_ON_ERROR = new MySQLKey<>("continueBatchOnError", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-statements.html">dontTrackOpenResources</a>
     * @since MySQL Driver  3.1.7
     */
    public static final MySQLKey<Boolean> DONT_TRACK_OPEN_RESOURCES = new MySQLKey<>("dontTrackOpenResources", Boolean.class, Boolean.FALSE);

    //TODO queryInterceptors

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-statements.html">queryTimeoutKillsConnection</a>
     * @since MySQL Driver  5.1.9
     */
    public static final MySQLKey<Boolean> QUERY_TIMEOUT_KILLS_CONNECTION = new MySQLKey<>("queryTimeoutKillsConnection", Boolean.class, Boolean.FALSE);

    /*-------------------below Prepared Statements group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">allowNanAndInf</a>
     * @since MySQL Driver  3.1.5
     */
    public static final MySQLKey<Boolean> ALLOW_NAN_AND_INF = new MySQLKey<>("allowNanAndInf", Boolean.class, Boolean.FALSE);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">allowNanAndInf</a>
//     * @deprecated jdbd don't support streams/readers
//     * @since MySQL Driver  3.1.12
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> AUTO_CLOSE_PSTMT_STREAMS = new MySQLKey<>("autoClosePStmtStreams", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">compensateOnDuplicateKeyUpdateCounts</a>
     * @since MySQL Driver  5.1.7
     */
    public static final MySQLKey<Boolean> COMPENSATE_ON_DUPLICATE_KEY_UPDATE_COUNTS = new MySQLKey<>("compensateOnDuplicateKeyUpdateCounts", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">emulateUnsupportedPstmts</a>
     * @since MySQL Driver  3.1.7
     */
    public static final MySQLKey<Boolean> EMULATE_UNSUPPORTED_PSTMTS = new MySQLKey<>("emulateUnsupportedPstmts", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">generateSimpleParameterMetadata</a>
     * @since MySQL Driver  5.0.5
     */
    public static final MySQLKey<Boolean> GENERATE_SIMPLE_PARAMETER_METADATA = new MySQLKey<>("generateSimpleParameterMetadata", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">processEscapeCodesForPrepStmts</a>
     * @since MySQL Driver  3.1.12
     */
    public static final MySQLKey<Boolean> PROCESS_ESCAPE_CODES_FOR_PREP_STMTS = new MySQLKey<>("processEscapeCodesForPrepStmts", Boolean.class, Boolean.TRUE);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">useServerPrepStmts</a>
//     * @deprecated jdbd don't need this option
//     * @since MySQL Driver  3.1.0
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> USE_SERVER_PREP_STMTS = new MySQLKey<>("useServerPrepStmts", Boolean.class, Boolean.FALSE);


//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-prepared-statements.html">useStreamLengthsInPrepStmts</a>
//     * @deprecated jdbd don't need this option
//     * @since MySQL Driver  3.0.2
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> useStreamLengthsInPrepStmts = new MySQLKey<>("useStreamLengthsInPrepStmts", Boolean.class, Boolean.TRUE);

    /*-------------------below Result Sets group-------------------*/

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">clobberStreamingResults</a>
//     * @since MySQL Driver  3.1.12
//     * @deprecated jdbd don't need this option
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> clobberStreamingResults = new MySQLKey<>("clobberStreamingResults", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">emptyStringsConvertToZero</a>
     * @since MySQL Driver  3.1.12
     */
    public static final MySQLKey<Boolean> EMPTY_STRINGS_CONVERT_TO_ZERO = new MySQLKey<>("emptyStringsConvertToZero", Boolean.class, Boolean.TRUE);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">holdResultsOpenOverStatementClose</a>
//     * @since MySQL Driver  3.1.7
//     * @deprecated jdbd don't need this option
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> HOLD_RESULTS_OPEN_OVER_STATEMENT_CLOSE = new MySQLKey<>("holdResultsOpenOverStatementClose", Boolean.class, Boolean.FALSE);

    /**
     * <p>
     * Jdbd throw {@link io.jdbd.JdbdException} not {@code java.sql.DataTruncation}
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">jdbcCompliantTruncation</a>
     * @since MySQL Driver  3.1.2
     */
    public static final MySQLKey<Boolean> JDBC_COMPLIANT_TRUNCATION = new MySQLKey<>("jdbcCompliantTruncation", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">maxRows</a>
     * @since MySQL Driver  all versions
     */
    public static final MySQLKey<Integer> MAX_ROWS = new MySQLKey<>("maxRows", Integer.class, 0);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">netTimeoutForStreamingResults</a>
     * @since MySQL Driver  5.1.0
     */
    public static final MySQLKey<Integer> NET_TIMEOUT_FOR_STREAMING_RESULTS = new MySQLKey<>("netTimeoutForStreamingResults", Integer.class, 600);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">padCharsWithSpace</a>
     * @since MySQL Driver  5.0.6
     */
    public static final MySQLKey<Boolean> PAD_CHARS_WITH_SPACE = new MySQLKey<>("padCharsWithSpace", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">populateInsertRowWithDefaultValues</a>
     * @since MySQL Driver  5.0.5
     */
    public static final MySQLKey<Boolean> POPULATE_INSERT_ROW_WITH_DEFAULT_VALUES = new MySQLKey<>("populateInsertRowWithDefaultValues", Boolean.class, Boolean.FALSE);


//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">scrollTolerantForwardOnly</a>
//     * @since MySQL Driver  8.0.24
//     * @deprecated jdbd don't need this option
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> SCROLL_TOLERANT_FORWARD_ONLY = new MySQLKey<>("scrollTolerantForwardOnly", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">strictUpdates</a>
     * @since MySQL Driver  3.0.4
     */
    public static final MySQLKey<Boolean> STRICT_UPDATES = new MySQLKey<>("strictUpdates", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">tinyInt1isBit</a>
     * @since MySQL Driver  3.0.16
     */
    public static final MySQLKey<Boolean> TINY_INT1_IS_BIT = new MySQLKey<>("tinyInt1isBit", Boolean.class, Boolean.TRUE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-result-sets.html">transformedBitIsBoolean</a>
     * @since MySQL Driver  3.1.9
     */
    public static final MySQLKey<Boolean> TRANS_FORMED_BIT_IS_BOOLEAN = new MySQLKey<>("transformedBitIsBoolean", Boolean.class, Boolean.FALSE);

    /*-------------------below Metadata group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html">getProceduresReturnsFunctions</a>
     * @since MySQL Driver  5.1.26
     */
    public static final MySQLKey<Boolean> GET_PROCEDURES_RETURNS_FUNCTIONS = new MySQLKey<>("getProceduresReturnsFunctions", Boolean.class, Boolean.TRUE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html">noAccessToProcedureBodies</a>
     * @since MySQL Driver  5.0.3
     */
    public static final MySQLKey<Boolean> NO_ACCESS_TO_PROCEDURE_BODIES = new MySQLKey<>("noAccessToProcedureBodies", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html">nullDatabaseMeansCurrent</a>
     * @since MySQL Driver  3.1.8
     */
    public static final MySQLKey<Boolean> NULL_DATABASE_MEANS_CURRENT = new MySQLKey<>("nullDatabaseMeansCurrent", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html">useHostsInPrivileges</a>
     * @since MySQL Driver  3.0.2
     */
    public static final MySQLKey<Boolean> USE_HOSTS_IN_PRIVILEGES = new MySQLKey<>("useHostsInPrivileges", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-metadata.html">useInformationSchema</a>
     * @since MySQL Driver  5.0.0
     */
    public static final MySQLKey<Boolean> USE_INFORMATION_SCHEMA = new MySQLKey<>("useInformationSchema", Boolean.class, Boolean.FALSE);

    /*-------------------below BLOB/CLOB processing group-------------------*/
//
//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">autoDeserialize</a>
//     * @since MySQL Driver  3.1.5
//     * @deprecated jdbd-mysql don't support
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> AUTO_DESERIALIZE = new MySQLKey<>("autoDeserialize", Boolean.class, Boolean.FALSE);

    /**
     * <p>
     *     <ul>
     *         <li>default: 1 << 20</li>
     *         <li>min : 1024</li>
     *         <li>max : {@link #MAX_ALLOWED_PACKET} - 4</li>
     *     </ul>
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">blobSendChunkSize</a>
     * @see #MAX_ALLOWED_PACKET
     * @since MySQL Driver  3.1.9
     */
    @Redefine
    public static final MySQLKey<Integer> BLOB_SEND_CHUNK_SIZE = new MySQLKey<>("blobSendChunkSize", Integer.class, 1 << 20); //1048576

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">blobsAreStrings</a>
     * @since MySQL Driver  5.0.8
     */
    public static final MySQLKey<Boolean> BLOBS_ARE_STRINGS = new MySQLKey<>("blobsAreStrings", Boolean.class, Boolean.FALSE);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">clobCharacterEncoding</a>
//     * @since MySQL Driver  5.0.0
//     * @deprecated jdbd-mysql don't need this option , see {@link io.jdbd.type.Text} and {@link io.jdbd.type.Clob}
//     */
//    @Deprecated
//    public static final MySQLKey<String> CLOB_CHARACTER_ENCODING = new MySQLKey<>("clobCharacterEncoding", String.class, null);


//    /**
//     *
//     *
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">emulateLocators</a>
//     * @see #LOCATOR_FETCH_BUFFER_SIZE
//     * @since MySQL Driver  3.1.0
//     * @deprecated jdbd is reactive ,dont' need this option.
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> EMULATE_LOCATORS = new MySQLKey<>("emulateLocators", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">functionsNeverReturnBlobs</a>
     * @since MySQL Driver  5.0.8
     */
    public static final MySQLKey<Boolean> FUNCTIONS_NEVER_RETURN_BLOBS = new MySQLKey<>("functionsNeverReturnBlobs", Boolean.class, Boolean.FALSE);

//    /**
//     *
//     *
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">locatorFetchBufferSize</a>
//     * @see #EMULATE_LOCATORS
//     * @since MySQL Driver  3.2.1
//     * @deprecated jdbd is reactive ,dont' need this option.
//     */
//    @Deprecated
//    public static final MySQLKey<Integer> LOCATOR_FETCH_BUFFER_SIZE = new MySQLKey<>("locatorFetchBufferSize", Integer.class, 1048576);

    /*-------------------below Datetime types processing group-------------------*/


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-datetime-types-processing.html">connectionTimeZone</a>
     * @since MySQL Driver  3.0.2
     */
    public static final MySQLKey<String> CONNECTION_TIME_ZONE = new MySQLKey<>("connectionTimeZone", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">forceConnectionTimeZoneToSession</a>
     * @since MySQL Driver  8.0.23
     */
    public static final MySQLKey<Boolean> FORCE_CONNECTION_TIME_ZONE_TO_SESSION = new MySQLKey<>("forceConnectionTimeZoneToSession", Boolean.class, Boolean.FALSE);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">noDatetimeStringSync</a>
//     * @deprecated don't support
//     * @since MySQL Driver  3.1.7
//     * @deprecated stupid ,jdbd-mysql don't support
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> noDatetimeStringSync = new MySQLKey<>("noDatetimeStringSync", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">preserveInstants</a>
     * @since MySQL Driver  8.0.23
     */
    public static final MySQLKey<Boolean> PRESERVE_INSTANTS = new MySQLKey<>("preserveInstants", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">sendFractionalSeconds</a>
     * @since MySQL Driver  5.1.37
     */
    public static final MySQLKey<Boolean> SEND_FRACTIONAL_SECONDS = new MySQLKey<>("sendFractionalSeconds", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">sendFractionalSecondsForTime</a>
     * @since MySQL Driver  8.0.23
     */
    public static final MySQLKey<Boolean> SEND_FRACTIONAL_SECONDS_FOR_TIME = new MySQLKey<>("sendFractionalSecondsForTime", Boolean.class, Boolean.TRUE);

//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">treatUtilDateAsTimestamp</a>
//     * @since MySQL Driver  5.0.5
//     * @deprecated jdbd don't need this option
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> TREAT_UTIL_DATE_AS_TIMESTAMP = new MySQLKey<>("treatUtilDateAsTimestamp", Boolean.class, Boolean.TRUE);


//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">yearIsDateType</a>
//     * @since MySQL Driver  3.1.9
//     * @deprecated jdbd don't need this option,java.time package dont need this option.
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> YEAR_IS_DATE_TYPE = new MySQLKey<>("yearIsDateType", Boolean.class, Boolean.TRUE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-blob-clob-processing.html">zeroDateTimeBehavior</a>
     * @since MySQL Driver  3.1.4
     */
    public static final MySQLKey<Enums.ZeroDatetimeBehavior> ZERO_DATE_TIME_BEHAVIOR = new MySQLKey<>("zeroDateTimeBehavior", Enums.ZeroDatetimeBehavior.class, Enums.ZeroDatetimeBehavior.EXCEPTION);

    /*-------------------below High Availability and Clustering group-------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">autoReconnect</a>
     * @see #MAX_RECONNECTS
     * @since MySQL Driver  1.1
     */
    public static final MySQLKey<Boolean> AUTO_RECONNECT = new MySQLKey<>("autoReconnect", Boolean.class, Boolean.FALSE);


//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">autoReconnectForPools</a>
//     * @since MySQL Driver  3.1.3
//     * @deprecated jdbd don't need this option
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> AUTO_RECONNECT_FOR_POOLS = new MySQLKey<>("autoReconnectForPools", Boolean.class, Boolean.FALSE);
//

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">failOverReadOnly</a>
     * @since MySQL Driver  3.0.12
     */
    public static final MySQLKey<Boolean> FAIL_OVER_READ_ONLY = new MySQLKey<>("failOverReadOnly", Boolean.class, Boolean.TRUE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">maxReconnects</a>
     * @see #AUTO_RECONNECT
     * @since MySQL Driver  1.1
     */
    public static final MySQLKey<Integer> MAX_RECONNECTS = new MySQLKey<>("maxReconnects", Integer.class, 3);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">reconnectAtTxEnd</a>
     * @since MySQL Driver  3.0.10
     */
    public static final MySQLKey<Boolean> RECONNECT_AT_TX_END = new MySQLKey<>("reconnectAtTxEnd", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">retriesAllDown</a>
     * @since MySQL Driver  5.1.6
     */
    public static final MySQLKey<Integer> RETRIES_ALL_DOWN = new MySQLKey<>("retriesAllDown", Integer.class, 120);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">initialTimeout</a>
     * @since MySQL Driver  1.1
     */
    public static final MySQLKey<Integer> INITIAL_TIMEOUT = new MySQLKey<>("initialTimeout", Integer.class, 2);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">queriesBeforeRetrySource</a>
     * @since MySQL Driver  3.0.2
     */
    public static final MySQLKey<Integer> QUERIES_BEFORE_RETRY_SOURCE = new MySQLKey<>("queriesBeforeRetrySource", Integer.class, 50);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">secondsBeforeRetrySource</a>
     * @since MySQL Driver 3.0.2
     */
    public static final MySQLKey<Integer> SECONDS_BEFORE_RETRY_SOURCE = new MySQLKey<>("secondsBeforeRetrySource", Integer.class, 30);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">allowReplicaDownConnections</a>
     * @since MySQL Driver 6.0.2
     */
    public static final MySQLKey<Boolean> ALLOW_REPLICA_DOWN_CONNECTIONS = new MySQLKey<>("allowReplicaDownConnections", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">allowSourceDownConnections</a>
     * @since MySQL Driver 5.1.27
     */
    public static final MySQLKey<Boolean> ALLOW_SOURCE_DOWN_CONNECTIONS = new MySQLKey<>("allowSourceDownConnections", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">ha.enableJMX</a>
     * @since MySQL Driver 5.1.27
     */
    public static final MySQLKey<Boolean> HA_ENABLE_JMX = new MySQLKey<>("ha.enableJMX", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceHostRemovalGracePeriod</a>
     * @since MySQL Driver 6.0.3
     */
    public static final MySQLKey<Integer> LOAD_BALANCE_HOST_REMOVAL_GRACE_PERIOD = new MySQLKey<>("loadBalanceHostRemovalGracePeriod", Integer.class, 15000);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">readFromSourceWhenNoReplicas</a>
     * @since MySQL Driver 6.0.2
     */
    public static final MySQLKey<Boolean> READ_FROM_SOURCE_WHEN_NO_REPLICAS = new MySQLKey<>("readFromSourceWhenNoReplicas", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">selfDestructOnPingMaxOperations</a>
     * @since MySQL Driver 5.1.6
     */
    public static final MySQLKey<Integer> SELF_DESTRUCT_ON_PING_MAX_OPERATIONS = new MySQLKey<>("selfDestructOnPingMaxOperations", Integer.class, 0);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">selfDestructOnPingSecondsLifetime</a>
     * @since MySQL Driver 5.1.6
     */
    public static final MySQLKey<Integer> SELF_DESTRUCT_ON_PING_SECONDS_LIFE_TIME = new MySQLKey<>("selfDestructOnPingSecondsLifetime", Integer.class, 0);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">ha.loadBalanceStrategy</a>
     * @since MySQL Driver 5.0.6
     */
    public static final MySQLKey<String> HA_LOAD_BALANCE_STRATEGY = new MySQLKey<>("ha.loadBalanceStrategy", String.class, "random");


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceAutoCommitStatementRegex</a>
     * @since MySQL Driver 5.1.15
     */
    public static final MySQLKey<String> LOAD_BALANCE_AUTO_COMMIT_STATEMENT_REGEX = new MySQLKey<>("loadBalanceAutoCommitStatementRegex", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceAutoCommitStatementThreshold</a>
     * @since MySQL Driver 5.1.15
     */
    public static final MySQLKey<Integer> LOAD_BALANCE_AUTO_COMMIT_STATEMENT_THRESHOLD = new MySQLKey<>("loadBalanceAutoCommitStatementThreshold", Integer.class, 0);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceBlocklistTimeout</a>
     * @since MySQL Driver 5.1.0
     */
    public static final MySQLKey<Integer> LOAD_BALANCE_BLOCK_LIST_TIMEOUT = new MySQLKey<>("loadBalanceBlocklistTimeout", Integer.class, 0);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceConnectionGroup</a>
     * @since MySQL Driver 5.1.13
     */
    public static final MySQLKey<String> LOAD_BALANCE_CONNECTION_GROUP = new MySQLKey<>("loadBalanceConnectionGroup", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceExceptionChecker</a>
     * @since MySQL Driver 5.1.13
     */
    public static final MySQLKey<String> LOAD_BALANCE_EXCEPTION_CHECKER = new MySQLKey<>("loadBalanceExceptionChecker", String.class, null);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalancePingTimeout</a>
     * @since MySQL Driver 5.1.13
     */
    public static final MySQLKey<Integer> LOAD_BALANCE_PING_TIMEOUT = new MySQLKey<>("loadBalancePingTimeout", Integer.class, 0);

    /**
     * TODO support ?
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceSQLExceptionSubclassFailover</a>
     * @since MySQL Driver 5.1.13
     */
    public static final MySQLKey<String> LOAD_BALANCE_SQL_EXCEPTION_SUBCLASS_FAILOVER = new MySQLKey<>("loadBalanceSQLExceptionSubclassFailover", String.class, null);

    /**
     * TODO support ?
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceSQLStateFailover</a>
     * @since MySQL Driver 5.1.13
     */
    public static final MySQLKey<String> LOAD_BALANCE_SQL_STATE_FAILOVER = new MySQLKey<>("loadBalanceSQLStateFailover", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">loadBalanceValidateConnectionOnSwapServer</a>
     * @since MySQL Driver 5.1.13
     */
    public static final MySQLKey<Boolean> LOAD_BALANCE_VALIDATE_CONNECTION_ON_SWAP_SERVER = new MySQLKey<>("loadBalanceValidateConnectionOnSwapServer", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">pinGlobalTxToPhysicalConnection</a>
     * @since MySQL Driver 5.0.1
     */
    public static final MySQLKey<Boolean> PIN_GLOBAL_TX_TO_PHYSICAL_CONNECTION = new MySQLKey<>("pinGlobalTxToPhysicalConnection", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">replicationConnectionGroup</a>
     * @since MySQL Driver 8.0.7
     */
    public static final MySQLKey<String> REPLICATION_CONNECTION_GROUP = new MySQLKey<>("replicationConnectionGroup", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">resourceId</a>
     * @since MySQL Driver 5.0.1
     */
    public static final MySQLKey<String> RESOURCE_ID = new MySQLKey<>("resourceId", String.class, null);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-high-availability-and-clustering.html">serverAffinityOrder</a>
     * @since MySQL Driver 8.0.8
     */
    public static final MySQLKey<String> SERVER_AFFINITY_ORDER = new MySQLKey<>("serverAffinityOrder", String.class, null);


    /*-------------------below Performance Extensions group-------------------*/

    /**
     * TODO support ?
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">callableStmtCacheSize</a>
     * @since MySQL Driver 3.1.2
     */
    public static final MySQLKey<Integer> CALLABLE_STMT_CACHE_SIZE = new MySQLKey<>("callableStmtCacheSize", Integer.class, 100);

    /**
     * TODO support ?
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">metadataCacheSize</a>
     * @since MySQL Driver 3.1.1
     */
    public static final MySQLKey<Integer> METADATA_CACHE_SIZE = new MySQLKey<>("metadataCacheSize", Integer.class, 50);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">useLocalSessionState</a>
     * @since MySQL Driver 3.1.7
     */
    public static final MySQLKey<Boolean> USE_LOCAL_SESSION_STATE = new MySQLKey<>("useLocalSessionState", Boolean.class, Boolean.FALSE);

    /**
     * TODO support ?
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">useLocalTransactionState</a>
     * @since MySQL Driver 5.1.7
     */
    public static final MySQLKey<Boolean> USE_LOCAL_TRANSACTION_STATE = new MySQLKey<>("useLocalTransactionState", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">prepStmtCacheSize</a>
     * @since MySQL Driver 3.0.10
     */
    public static final MySQLKey<Integer> PREP_STMT_CACHE_SIZE = new MySQLKey<>("prepStmtCacheSize", Integer.class, 25);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">prepStmtCacheSqlLimit</a>
     * @since MySQL Driver 3.0.10
     */
    public static final MySQLKey<Integer> PREP_STMT_CACHE_SQL_LIMIT = new MySQLKey<>("prepStmtCacheSqlLimit", Integer.class, 256);


//    /**
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">queryInfoCacheFactory</a>
//     * @since MySQL Driver 5.1.1
//     * @deprecated jdbd don't need this option
//     */
//    @Deprecated
//    public static final MySQLKey<String> QUERY_INFO_CACHE_FACTORY = new MySQLKey<>("queryInfoCacheFactory", String.class, null);
//

    /**
     * TODO support ?
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">alwaysSendSetIsolation</a>
     * @since MySQL Driver 3.1.7
     */
    public static final MySQLKey<Boolean> ALWAYS_SEND_SET_ISOLATION = new MySQLKey<>("alwaysSendSetIsolation", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">maintainTimeStats</a>
     * @since MySQL Driver 3.1.9
     */
    public static final MySQLKey<Boolean> MAINTAIN_TIME_STATS = new MySQLKey<>("maintainTimeStats", Boolean.class, Boolean.TRUE);

//    /**
//     *
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">useCursorFetch</a>
//     * @since MySQL Driver 5.0.0
//     * @deprecated jdbd don't need this option,jdbd is reactive .
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> USE_CURSOR_FETCH = new MySQLKey<>("useCursorFetch", Boolean.class, Boolean.FALSE);
//

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">cacheCallableStmts</a>
     * @since MySQL Driver 3.1.2
     */
    public static final MySQLKey<Boolean> CACHE_CALLABLE_STMTS = new MySQLKey<>("cacheCallableStmts", Boolean.class, Boolean.FALSE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">cachePrepStmts</a>
     * @since MySQL Driver 3.0.10
     */
    public static final MySQLKey<Boolean> CACHE_PREP_STMTS = new MySQLKey<>("cachePrepStmts", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">cacheResultSetMetadata</a>
     * @since MySQL Driver 3.1.1
     */
    public static final MySQLKey<Boolean> CACHE_RESULT_SET_METADATA = new MySQLKey<>("cacheResultSetMetadata", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">cacheServerConfiguration</a>
     * @since MySQL Driver 3.1.5
     */
    public static final MySQLKey<Boolean> CACHE_SERVER_CONFIGURATION = new MySQLKey<>("cacheServerConfiguration", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">defaultFetchSize</a>
     * @since MySQL Driver 3.1.9
     */
    public static final MySQLKey<Integer> DEFAULT_FETCH_SIZE = new MySQLKey<>("defaultFetchSize", Integer.class, 0);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">dontCheckOnDuplicateKeyUpdateInSQL</a>
     * @since MySQL Driver 5.1.32
     */
    public static final MySQLKey<Boolean> DONT_CHECK_ON_DUPLICATE_KEY_UPDATE_IN_SQL = new MySQLKey<>("dontCheckOnDuplicateKeyUpdateInSQL", Boolean.class, Boolean.FALSE);


//    /**
//     *
//     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">elideSetAutoCommits</a>
//     * @since MySQL Driver 3.1.3
//     * @deprecated jdbd don't this option
//     */
//    @Deprecated
//    public static final MySQLKey<Boolean> ELIDE_SET_AUTO_COMMITS = new MySQLKey<>("elideSetAutoCommits", Boolean.class, Boolean.FALSE);
//

    /**
     * <p>
     *     TODO support ?   jdbd must escape
     * </p>
     *
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">enableEscapeProcessing</a>
     * @since MySQL Driver 6.0.1
     */
    public static final MySQLKey<Boolean> ENABLE_ESCAPE_PROCESSING = new MySQLKey<>("enableEscapeProcessing", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">enableQueryTimeouts</a>
     * @since MySQL Driver 5.0.6
     */
    public static final MySQLKey<Boolean> ENABLE_QUERY_TIMEOUTS = new MySQLKey<>("enableQueryTimeouts", Boolean.class, Boolean.TRUE);


    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">largeRowSizeThreshold</a>
     * @since MySQL Driver 5.1.1
     */
    public static final MySQLKey<Integer> LARGE_ROW_SIZE_THRESHOLD = new MySQLKey<>("largeRowSizeThreshold", Integer.class, 2048);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">readOnlyPropagatesToServer</a>
     * @since MySQL Driver 5.1.35
     */
    public static final MySQLKey<Boolean> READ_ONLY_PROPAGATES_TO_SERVER = new MySQLKey<>("readOnlyPropagatesToServer", Boolean.class, Boolean.TRUE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">rewriteBatchedStatements</a>
     * @since MySQL Driver 3.1.13
     */
    public static final MySQLKey<Boolean> REWRITE_BATCHED_STATEMENTS = new MySQLKey<>("rewriteBatchedStatements", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">useReadAheadInput</a>
     * @since MySQL Driver 3.1.5
     */
    public static final MySQLKey<Boolean> USE_READ_AHEAD_INPUT = new MySQLKey<>("useReadAheadInput", Boolean.class, Boolean.TRUE);

    /**
     * <p>
     * jdbd-mysql defined. min value is (0xFFFF_FF * 10) bytes .
     * </p>
     *
     * @since jdbd-mysql 1.0
     */
    public static final MySQLKey<Integer> BIG_COLUMN_BOUNDARY_BYTES = new MySQLKey<>("bigColumnBoundaryBytes", Integer.class, 1 << 30);

    /*-------------------below Debugging/Profiling group-------------------*/

    // logger ; don't support

    // profilerEventHandler ; don't support

    // useNanosForElapsedTime ; don't support

    // maxQuerySizeToLog ; don't support


    /*-------------------below Exceptions/Warnings group -------------------*/

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">ignoreNonTxTables</a>
     * @since MySQL Driver 3.0.9
     */
    public static final MySQLKey<Boolean> IGNORE_NON_TX_TABLES = new MySQLKey<>("ignoreNonTxTables", Boolean.class, Boolean.FALSE);

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-performance-extensions.html">includeInnodbStatusInDeadlockExceptions</a>
     * @since MySQL Driver 5.0.7
     */
    public static final MySQLKey<Boolean> INCLUDE_INNODB_STATUS_IN_DEAD_LOCK_EXCEPTIONS = new MySQLKey<>("includeInnodbStatusInDeadlockExceptions", Boolean.class, Boolean.FALSE);

    //TODO add other Exceptions/Warnings option


    //below  Group

    /**
     * The hostname value from the properties instance passed to the driver.
     */
    public static final MySQLKey<String> HOST = new MySQLKey<>("host", String.class, null);

    /**
     * The port number value from the properties instance passed to the driver.
     */
    public static final MySQLKey<Integer> PORT = new MySQLKey<>("port", Integer.class, 3306);

    /**
     * The communications protocol. Possible values: "tcp" and "pipe".
     */
    public static final MySQLKey<String> PROTOCOL = new MySQLKey<>("protocol", String.class, null);

    /**
     * The name pipes path to use when "protocol=pipe'.
     */
    public static final MySQLKey<String> PATH = new MySQLKey<>("path", String.class, "namedPipePath");

    /**
     * The server type in a replication setup. Possible values: "master" and "slave".
     */
    public static final MySQLKey<String> TYPE = new MySQLKey<>("type", String.class, null);

    /**
     * The address value ("host:port") from the properties instance passed to the driver.
     */
    public static final MySQLKey<String> ADDRESS = new MySQLKey<>("address", String.class, null);

    /**
     * The host priority in a list of hosts.
     */
    public static final MySQLKey<String> PRIORITY = new MySQLKey<>("priority", String.class, null);

    /**
     * The database value from the properties instance passed to the driver.
     */
    public static final MySQLKey<String> DB_NAME = new MySQLKey<>("dbname", String.class, null);

    // blow Connection Group

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html">Server SQL Modes</a>
     */
    public static final MySQLKey<String> APPEND_SQL_MODE = new MySQLKey<>("append.sql.mode", String.class, null);

    public static final MySQLKey<Integer> FACTORY_WORKER_COUNT = new MySQLKey<>("factory_work_count", Integer.class, 50);

    public static final MySQLKey<Integer> FACTORY_TASK_QUEUE_SIZE = new MySQLKey<>("factory_task_queue_size", Integer.class, 18);

    public static final MySQLKey<String> FACTORY_NAME = new MySQLKey<>(Driver.FACTORY_NAME, String.class, "unnamed");


    private MySQLKey(String name, Class<T> valueClass, @Nullable T defaultValue) {
        super(name, valueClass, defaultValue);
    }


}
