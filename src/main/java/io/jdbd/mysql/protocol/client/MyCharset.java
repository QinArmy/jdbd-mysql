package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.protocol.MySQLServerVersion;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * @see Charsets
 * @since 1.0
 */
public final class MyCharset {

    public final String name;
    public final int mblen;
    public final int priority;
    public final List<String> javaEncodingsUcList;

    public final MySQLServerVersion minimumVersion;

    /**
     * Constructs MySQLCharset object
     *
     * @param name          MySQL charset name
     * @param mblen         Max number of bytes per character
     * @param priority      MySQLCharset with highest value of this param will be used for Java encoding --&gt; Mysql charsets conversion.
     * @param javaEncodings List of Java encodings corresponding to this MySQL charset; the first name in list is the default for mysql --&gt; java data conversion
     */
    MyCharset(String name, int mblen, int priority, String... javaEncodings) {
        this(name, mblen, priority, MySQLServerVersion.getMinVersion(), javaEncodings);
    }

    MyCharset(String name, int mblen, int priority, MySQLServerVersion minimumVersion
            , String... javaEncodings) {
        this.name = name;
        this.mblen = mblen;
        this.priority = priority;
        this.javaEncodingsUcList = createJavaEncodingUcList(mblen, javaEncodings);
        this.minimumVersion = minimumVersion;
    }

    public String charsetName() {
        return this.name;
    }

    public MyCharset self() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder asString = new StringBuilder();
        asString.append("[");
        asString.append("charsetName=");
        asString.append(this.name);
        asString.append(",mblen=");
        asString.append(this.mblen);
        // asString.append(",javaEncoding=");
        // asString.append(this.javaEncodings.toString());
        asString.append("]");
        return asString.toString();
    }

    boolean isOkayForVersion(MySQLServerVersion version) {
        return version.meetsMinimum(this.minimumVersion);
    }

    /**
     * If javaEncoding parameter value is one of available java encodings for this charset
     * then returns javaEncoding value as is. Otherwise returns first available java encoding name.
     *
     * @param javaEncoding java encoding name
     * @return java encoding name
     */
    String getMatchingJavaEncoding(@Nullable String javaEncoding) {
        if (javaEncoding != null && this.javaEncodingsUcList.contains(javaEncoding.toUpperCase(Locale.ENGLISH))) {
            return javaEncoding;
        }
        return this.javaEncodingsUcList.get(0);
    }

    private static void addEncodingMapping(List<String> javaEncodingsUc, String encoding) {
        String encodingUc = encoding.toUpperCase(Locale.ENGLISH);

        if (!javaEncodingsUc.contains(encodingUc)) {
            javaEncodingsUc.add(encodingUc);
        }
    }

    /**
     * @return a unmodifiable list
     */
    private static List<String> createJavaEncodingUcList(final int mblen, String... javaEncodings) {
        List<String> javaEncodingsUcList = new ArrayList<>(javaEncodings.length);
        for (String encoding : javaEncodings) {
            try {
                Charset cs = Charset.forName(encoding);
                addEncodingMapping(javaEncodingsUcList, cs.name());
                for (String alias : cs.aliases()) {
                    addEncodingMapping(javaEncodingsUcList, alias);
                }
            } catch (Exception e) {
                // if there is no support of this charset in JVM it's still possible to use our converter for 1-byte charsets
                if (mblen == 1) {
                    addEncodingMapping(javaEncodingsUcList, encoding);
                }
            }
        }

        if (javaEncodingsUcList.size() == 0) {
            if (mblen > 1) {
                addEncodingMapping(javaEncodingsUcList, "UTF-8");
            } else {
                addEncodingMapping(javaEncodingsUcList, "Cp1252");
            }
        }
        if (javaEncodingsUcList.size() == 1) {
            javaEncodingsUcList = Collections.singletonList(javaEncodingsUcList.get(0));
        } else {
            javaEncodingsUcList = Collections.unmodifiableList(javaEncodingsUcList);
        }
        return javaEncodingsUcList;
    }
}
