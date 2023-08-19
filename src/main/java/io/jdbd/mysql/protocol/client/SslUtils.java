package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.util.MySQLCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

abstract class SslUtils {

    protected SslUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger LOG = LoggerFactory.getLogger(SslUtils.class);


    static final String TLSv1 = "TLSv1";
    static final String TLSv1_1 = "TLSv1.1";
    static final String TLSv1_2 = "TLSv1.2";
    static final String TLSv1_3 = "TLSv1.3";

    /** a unmodifiable list */
    static final List<String> MYSQL_RESTRICTED_CIPHER_SUBSTR_LIST = createMySQLRestrictedCipherSubStrList();

    /** a unmodifiable list */
    static final List<String> CLIENT_SUPPORT_TLS_PROTOCOL_LIST = createClientSupportTlsProtocolList();

    /** a unmodifiable list */
    static final List<String> CLIENT_SUPPORT_TLS_CIPHER_LIST = createClientSupportCipherSuitList();

    /**
     * @return a unmodifiable list
     */
    private static List<String> createClientSupportCipherSuitList() {
        List<String> list = createMySQLSupportTlsCipherSuitList();
        return Collections.unmodifiableList(list);
    }

    /**
     * @return a unmodifiable list
     */
    private static List<String> createClientSupportTlsProtocolList() {
        List<String> list = createMySQLSupportTlsProtocolList();
        list.retainAll(createJdkSupportTlsProtocolList());
        return Collections.unmodifiableList(list);
    }

    /**
     * @return a unmodifiable list
     */
    private static List<String> createMySQLRestrictedCipherSubStrList() {
        List<String> list = MySQLCollections.arrayList(8);
        // Unacceptable TLS Ciphers
        list.add("_ANON_");
        list.add("_NULL_");
        list.add("_EXPORT");
        list.add("_MD5");

        list.add("_DES");
        list.add("_RC2_");
        list.add("_RC4_");
        list.add("_PSK_");
        return Collections.unmodifiableList(list);
    }


    /**
     * @return a modifiable list
     */
    private static List<String> createMySQLSupportTlsProtocolList() {
        List<String> list = MySQLCollections.arrayList(4);
        list.add(TLSv1_3);
        list.add(TLSv1_2);
        list.add(TLSv1_1);
        list.add(TLSv1);
        return list;
    }

    /**
     * @return a modifiable list
     */
    static List<String> createMySQLSupportTlsCipherSuitList() {
        List<String> list = MySQLCollections.arrayList();

        // Mandatory TLS Ciphers");

        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");


        // Approved TLS Ciphers");
        list.add("TLS_AES_128_GCM_SHA256");
        list.add("TLS_AES_256_GCM_SHA384");
        list.add("TLS_CHACHA20_POLY1305_SHA256");
        list.add("TLS_AES_128_CCM_SHA256");

        list.add("TLS_AES_128_CCM_8_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DHE_DSS_WITH_AES_128_GCM_SHA256");

        list.add("TLS_DHE_DSS_WITH_AES_256_GCM_SHA384");
        list.add("TLS_DHE_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256");

        list.add("TLS_DH_DSS_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_DSS_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_GCM_SHA384");

        list.add("TLS_DH_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_ECDH_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_DH_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_ECDH_RSA_WITH_AES_256_GCM_SHA384");


        // Deprecated TLS Ciphers");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");

        list.add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA256");
        list.add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA256");

        list.add("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_DHE_DSS_WITH_AES_128_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_DH_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_DH_RSA_WITH_AES_256_CBC_SHA256");

        list.add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_DH_DSS_WITH_AES_128_CBC_SHA256");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384");
        list.add("TLS_DH_DSS_WITH_AES_128_CBC_SHA");

        list.add("TLS_ECDH_ECDSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_DH_DSS_WITH_AES_256_CBC_SHA256");

        list.add("TLS_DH_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_ECDH_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_ECDH_RSA_WITH_AES_256_CBC_SHA");

        list.add("TLS_RSA_WITH_AES_128_GCM_SHA256");
        list.add("TLS_RSA_WITH_AES_256_GCM_SHA384");
        list.add("TLS_RSA_WITH_AES_128_CBC_SHA256");
        list.add("TLS_RSA_WITH_AES_256_CBC_SHA256");

        list.add("TLS_RSA_WITH_AES_128_CBC_SHA");
        list.add("TLS_RSA_WITH_AES_256_CBC_SHA");
        list.add("TLS_RSA_WITH_CAMELLIA_256_CBC_SHA");
        list.add("TLS_RSA_WITH_CAMELLIA_128_CBC_SHA");

        list.add("TLS_DH_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DH_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_DSS_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_DHE_RSA_WITH_3DES_EDE_CBC_SHA");

        list.add("TLS_ECDH_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDH_ECDSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA");
        list.add("TLS_ECDHE_ECDSA_WITH_3DES_EDE_CBC_SHA");

        list.add("TLS_RSA_WITH_3DES_EDE_CBC_SHA");

        return list;
    }


    /**
     * @return a modifiable list
     * @see <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/StandardNames.html#SSLContext">JDK TLS protocls</a>
     */
    private static List<String> createJdkSupportTlsProtocolList() {
        List<String> mySqlSupportTlsProtocolList = createMySQLSupportTlsProtocolList();
        List<String> list = MySQLCollections.arrayList(mySqlSupportTlsProtocolList.size());
        for (String protocol : mySqlSupportTlsProtocolList) {
            try {
                SSLContext.getInstance(protocol);
                list.add(protocol);
            } catch (NoSuchAlgorithmException e) {
                // test failure ,ignore.
                LOG.debug("{} unsupported by JDK,ignore.", protocol);
            }
        }
        return list;
    }


}
