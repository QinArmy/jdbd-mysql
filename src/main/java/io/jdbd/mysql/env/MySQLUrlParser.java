package io.jdbd.mysql.env;

import io.jdbd.Driver;
import io.jdbd.JdbdException;
import io.jdbd.lang.Nullable;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.JdbdHost;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * <p>
 * This class is responsible for parsing MySQL url.
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-jdbc-url-format.html">Connection URL Syntax</a>
 */
public abstract class MySQLUrlParser {

    private MySQLUrlParser() {
        throw new UnsupportedOperationException();
    }

    private static final Pattern CONNECTION_STRING_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+)\\s*" // scheme: required; alphanumeric, plus, colon or percent
            + "(?://(?<authority>[^/?#]*))?\\s*" // authority: optional; starts with "//" followed by any char except "/", "?" and "#"
            + "(?:/(?!\\s*/)(?<path>[^?#]*))?" // path: optional; starts with "/" but not followed by "/", and then followed by by any char except "?" and "#"
            + "(?:\\?(?!\\s*\\?)(?<query>[^#]*))?" // query: optional; starts with "?" but not followed by "?", and then followed by by any char except "#"
            + "(?:\\s*#(?<fragment>.*))?"); // fragment: optional; starts with "#", and then followed by anything


    private static final Pattern SCHEME_PTRN = Pattern.compile("(?<scheme>[\\w\\+:%]+).*");


    private static final short DEFAULT_PORT = 3306;


    public static boolean acceptsUrl(final @Nullable String url) {
        if (url == null) {
            return false;
        }
        boolean accept = false;
        for (Protocol protocol : Protocol.values()) {
            if (url.startsWith(protocol.scheme)) {
                accept = true;
                break;
            }
        }
        return accept;
    }


    public static List<MySQLHost> parse(final String url, final Map<String, Object> properties) {
        if (!isConnectionStringSupported(url)) {
            String m = String.format("unsupported url[%s] schema", url);
            throw new JdbdException(m);
        }
        final Matcher matcher = CONNECTION_STRING_PTRN.matcher(url);
        if (!matcher.matches()) {
            String m = String.format("url[%s] schema not match.", url);
            throw new JdbdException(m);
        }

        final String schema, authority, path, query, actualAuthority;

        try {
            // 1. parse url partition.
            schema = decodeSkippingPlusSign(matcher.group("scheme"));
            authority = matcher.group("authority"); // Don't decode just yet.
            path = matcher.group("path") == null ? null : decode(matcher.group("path")).trim();
            query = matcher.group("query"); // Don't decode just yet.

            // 2-1 parse url query properties
            final Map<String, Object> queryProperties;
            queryProperties = parseQueryProperties(query);

            if (path != null) {
                queryProperties.put(MySQLKey.DB_NAME.name, path);
            }

            //2-2 parse user and password from url
            if (properties.containsKey(Driver.USER)) {
                actualAuthority = authority;
            } else {
                actualAuthority = parseUserInfo(authority, queryProperties);
            }


            //3. override query properties wih properties
            queryProperties.putAll(properties);
            queryProperties.putIfAbsent(MySQLKey.HOST.name, JdbdHost.DEFAULT_HOST);
            queryProperties.putIfAbsent(MySQLKey.PORT.name, DEFAULT_PORT);


            //4. parse MySQL host  list
            return parseHostList(url, schema, actualAuthority, queryProperties);
        } catch (JdbdException e) {
            throw e;
        } catch (Throwable e) {
            throw MySQLExceptions.wrap(e);
        }
    }

    /*-------------------below private method -------------------*/


    private static List<MySQLHost> parseHostList(final String url, final String schema, final String authority,
                                                 final Map<String, Object> queryProperties) {
        final Protocol protocol;

        final List<MySQLHost> hostList;
        if (MySQLStrings.hasText(authority)) {
            final List<Map<String, Object>> propertyList;
            propertyList = parseHostPropertyList(authority);
            protocol = Protocol.fromValue(url, schema, propertyList.size());

            final List<MySQLHost> tempList = MySQLCollections.arrayList(propertyList.size());
            Map<String, Object> map;
            for (Map<String, Object> hostProperties : propertyList) {
                map = MySQLCollections.hashMap(queryProperties);
                map.putAll(hostProperties);// override query properties wih host
                tempList.add(MySQLJdbdHost.create(protocol, map));
            }
            hostList = MySQLCollections.unmodifiableList(tempList);
        } else {
            protocol = Protocol.fromValue(url, schema, 1);
            hostList = Collections.singletonList(MySQLJdbdHost.create(protocol, queryProperties));
        }
        return hostList;
    }


    /**
     * @return a unmodifiable list
     */
    private static List<Map<String, Object>> parseHostPropertyList(String multiHostsSegment) {
        if (multiHostsSegment.startsWith("[") && multiHostsSegment.endsWith("]")) {
            multiHostsSegment = multiHostsSegment.substring(1, multiHostsSegment.length() - 1);
        }
        final String authority = multiHostsSegment;
        final List<Character> markList = obtainMarkList();
        final int len = authority.length();

        final List<Map<String, Object>> hostPropertiesList = MySQLCollections.arrayList();

        char openingMarker;
        for (int openingMarkIndex = MySQLStrings.indexNonSpace(authority); openingMarkIndex > -1; ) {
            openingMarker = authority.charAt(openingMarkIndex);
            if (openingMarker == '(') {
                // this 'if'  block for key-value host
                int index = authority.indexOf(')', openingMarkIndex);
                if (index < 0) {
                    String m = String.format("%s no closing mark", authority.substring(openingMarkIndex));
                    throw new JdbdException(m);
                }
                index++; // right shift to comma or ending.
                hostPropertiesList.add(parseKeyValueHost(authority.substring(openingMarkIndex, index)));
                index = MySQLStrings.indexNonSpace(authority, index);
                if (index < 0) {
                    break;
                }
                if (authority.charAt(index) != ',') {
                    throw createFormatException(authority.substring(openingMarkIndex, index + 1));
                }
                openingMarkIndex = MySQLStrings.indexNonSpace(authority, index + 1);
                if (openingMarkIndex < 0) {
                    throw createAuthorityEndWithCommaException(multiHostsSegment);
                }
            } else if (isAddressEqualsHostPrefix(authority, openingMarkIndex)) {
                // this 'if'  block for address equals host
                int commaIndex = indexAddressEqualsHostSegmentEnding(authority, openingMarkIndex);
                if (commaIndex < 0) {
                    hostPropertiesList.add(parseAddressEqualsHost(authority.substring(openingMarkIndex)));
                    break;
                } else {
                    hostPropertiesList.add(parseAddressEqualsHost(authority.substring(openingMarkIndex, commaIndex)));
                    openingMarkIndex = MySQLStrings.indexNonSpace(authority, commaIndex + 1);
                    if (openingMarkIndex < 0) {
                        throw createAuthorityEndWithCommaException(multiHostsSegment);
                    }
                }
            } else if (markList.contains(authority.charAt(openingMarkIndex))) {
                throw createFormatException(authority.substring(openingMarkIndex == 0 ? 0 : openingMarkIndex - 1));
            } else {
                // this 'else'  block for host-port host
                int commaIndex = -1;
                for (int i = openingMarkIndex + 1; i < len; i++) {
                    char ch = authority.charAt(i);
                    if (ch == ',') {
                        commaIndex = i;
                        break;
                    } else if (markList.contains(ch)) {
                        throw createFormatException(authority.substring(openingMarkIndex, i + 1));
                    }
                }
                if (commaIndex < 0) {
                    hostPropertiesList.add(parseHostPortHost(authority.substring(openingMarkIndex)));
                    break;
                } else {
                    hostPropertiesList.add(parseHostPortHost(authority.substring(openingMarkIndex, commaIndex)));
                    openingMarkIndex = MySQLStrings.indexNonSpace(authority, commaIndex + 1);
                    if (openingMarkIndex < 0) {
                        throw createAuthorityEndWithCommaException(multiHostsSegment);
                    }
                }
            }
        }

        return MySQLCollections.unmodifiableList(hostPropertiesList);
    }


    /**
     * @return a unmodifiable map
     * @see #parseHostPropertyList(String)
     */
    private static Map<String, Object> parseAddressEqualsHost(final String addressEqualsHost) {
        int openingMarkersIndex = addressEqualsHost.indexOf('(');
        if (openingMarkersIndex < 0) {
            throw new IllegalArgumentException(String.format("addressEqualsHost[%s] error.", addressEqualsHost));
        }

        final int originalOpeningMarkersIndex = openingMarkersIndex;
        int closingMarkersIndex;
        Map<String, Object> hostKeyValueMap = MySQLCollections.hashMap();
        while ((closingMarkersIndex = addressEqualsHost.indexOf(')', openingMarkersIndex)) > 0) {
            String pair = addressEqualsHost.substring(openingMarkersIndex + 1, closingMarkersIndex);
            String[] kv = pair.split("=");
            if (kv.length != 2) {
                throw createFormatException(addressEqualsHost);
            }
            hostKeyValueMap.put(decode(kv[0]).trim(), decode(kv[1]).trim());
            openingMarkersIndex = addressEqualsHost.indexOf('(', closingMarkersIndex);
            if (openingMarkersIndex < 0) {
                break;
            }
        }
        if (openingMarkersIndex == originalOpeningMarkersIndex) {
            // not found key value pair.
            throw createFormatException(addressEqualsHost);
        }
        return MySQLCollections.unmodifiableMap(hostKeyValueMap);
    }

    /**
     * @return a unmodifiable map
     */
    private static Map<String, Object> parseKeyValueHost(String keyValueHost) {
        int openingMarkersIndex = keyValueHost.indexOf('(');
        int closingMarkersIndex = keyValueHost.lastIndexOf(')');

        if (openingMarkersIndex < 0 || closingMarkersIndex < 0) {
            throw createFormatException(keyValueHost);
        }
        keyValueHost = keyValueHost.substring(openingMarkersIndex + 1, closingMarkersIndex);
        String[] pairArray = keyValueHost.split(",");

        if (pairArray.length == 0) {
            throw createFormatException(keyValueHost);
        }
        final Map<String, Object> hostKeyValueMap = MySQLCollections.hashMap((int) (pairArray.length / 0.75F));
        String[] kv;
        for (String pair : pairArray) {
            kv = pair.split("=");
            if (kv.length != 2) {
                throw createFormatException(keyValueHost);
            }
            hostKeyValueMap.put(decode(kv[0]).trim(), decode(kv[1]).trim());
        }
        return MySQLCollections.unmodifiableMap(hostKeyValueMap);
    }


    private static String parseUserInfo(final String authority, final Map<String, Object> properties) {
        final int index;
        index = authority.indexOf('@');
        if (index < 0) {
            return authority;
        }
        final String userInfo;
        userInfo = authority.substring(0, index);
        final String[] userInfoPair;
        userInfoPair = userInfo.split(":");
        if (userInfoPair.length == 0 || userInfoPair.length > 2) {
            throw new JdbdException(String.format("user info[%s] error of url.", userInfo));
        }
        try {
            properties.put(Driver.USER, URLDecoder.decode(userInfoPair[0], "UTF-8"));
            if (userInfoPair.length == 2) {
                properties.put(Driver.PASSWORD, URLDecoder.decode(userInfoPair[1], "UTF-8"));
            }
        } catch (UnsupportedEncodingException e) {
            //never here
            throw new JdbdException("Unsupported charset", e);
        }
        return authority.substring(index + 1);
    }


    /**
     * @return a unmodifiable map
     * @see #parseHostPropertyList(String)
     */
    private static Map<String, Object> parseHostPortHost(final String hostPortHost) {
        String[] hostPortPair = hostPortHost.split(":");
        if (hostPortPair.length == 0 || hostPortPair.length > 2) {
            throw createFormatException(hostPortHost);
        }
        Map<String, Object> hostKeyValueMap = MySQLCollections.hashMap(4);
        hostKeyValueMap.put(MySQLKey.HOST.name, decode(hostPortPair[0]).trim());

        if (hostPortPair.length == 2) {
            hostKeyValueMap.put(MySQLKey.PORT.name, decode(hostPortPair[1]).trim());
        }
        return Collections.unmodifiableMap(hostKeyValueMap);
    }


    /**
     * @param openingMark index of address-equals host prefix{@code pattern 'address\s*=\s*('}
     */
    private static int indexAddressEqualsHostSegmentEnding(final String authority, final int openingMark) {
        final int prefixEndIndex = authority.indexOf('(', openingMark);
        if (prefixEndIndex < 0) {
            throw new IllegalArgumentException("openingMark isn't address-equals host prefix index.");
        }
        final int len = authority.length();
        boolean close = false;
        int separator = -1;
        for (int i = prefixEndIndex + 1; i < len; i++) {
            if (authority.charAt(i) == ')') {
                if (close) {
                    throw createParenthesisNotMatchException(authority.substring(openingMark, i + 1));
                }
                close = true;
            } else if (authority.charAt(i) == '(') {
                if (!close) {
                    throw createParenthesisNotMatchException(authority.substring(openingMark, i + 1));
                }
                close = false;
            } else if (authority.charAt(i) == ',') {
                if (!close) {
                    throw createParenthesisNotMatchException(authority.substring(openingMark, i + 1));
                }
                separator = i;
                break;
            }
        }
        if (!close) {
            throw createParenthesisNotMatchException(authority.substring(openingMark));
        }
        return separator;
    }


    /**
     * @return a modifiable map
     */
    private static Map<String, Object> parseQueryProperties(final String query) {
        final Map<String, Object> properties = MySQLCollections.hashMap();

        if (MySQLStrings.isEmpty(query)) {
            return properties;
        }
        final String[] queryPairs;
        queryPairs = query.split("&");
        String[] kv;
        for (String pair : queryPairs) {
            kv = pair.split("=");
            if (kv.length == 0 || kv.length > 2) {
                throw new JdbdException(String.format("query[%s] error of url.", query));
            }
            if (kv.length == 2) {
                properties.put(decode(kv[0]).trim(), decode(kv[1]).trim());
            }
        }
        return properties;
    }


    private static boolean isAddressEqualsHostPrefix(String segment, final int fromIndex) {
        final String address = "address";
        if (fromIndex < 0 || !segment.startsWith(address, fromIndex)) {
            return false;
        }
        final int len = segment.length();
        final char space = '\u0020';
        char ch;
        boolean match = false;
        for (int i = fromIndex + address.length(), charCount = 0; i < len; i++) {
            ch = segment.charAt(i);
            if (ch == space) {
                continue;
            }
            if (ch == '=' && charCount == 0) {
                charCount++;
            } else if (ch == '(' && charCount == 1) {
                match = true;
                break;
            } else {
                break;
            }
        }
        return match;
    }


    /**
     * Checks if the scheme part of given connection string matches one of the {@link Protocol}s supported by Connector/J.
     *
     * @param connString connection string
     * @return true if supported
     */
    private static boolean isConnectionStringSupported(String connString) {
        Matcher matcher = SCHEME_PTRN.matcher(connString);
        return matcher.matches() && Protocol.isSupported(decodeSkippingPlusSign(matcher.group("scheme")));
    }

    /**
     * URL-decode the given string skipping all occurrences of the plus sign.
     *
     * @param text the string to decode
     * @return the decoded string
     */
    private static String decodeSkippingPlusSign(String text) {
        if (text.isEmpty()) {
            return text;
        }
        text = text.replace("+", "%2B"); // Percent encode for "+" is "%2B".
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
            throw new JdbdException(e.getMessage(), e);
        }
    }


    /**
     * URL-decode the given string.
     *
     * @param text the string to decode
     * @return the decoded string
     */
    private static String decode(String text) {
        if (MySQLStrings.isEmpty(text)) {
            return text;
        }
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new JdbdException(e.getMessage(), e);
        }
    }


    private static List<Character> obtainMarkList() {
        List<Character> chList = MySQLCollections.arrayList(4);
        chList.add(',');
        chList.add('=');
        chList.add('(');
        chList.add(')');
        return Collections.unmodifiableList(chList);
    }


    private static JdbdException createAuthorityEndWithCommaException(String authority) {
        String m = String.format("\"%s\" can't end with comma", authority);
        return new JdbdException(m);
    }

    private static JdbdException createParenthesisNotMatchException(String hostSegment) {
        String m = String.format("\"%s\" parenthesis count not match.", hostSegment);
        return new JdbdException(m);
    }

    private static JdbdException createFormatException(String hostSegment) {
        String m = String.format("\"%s\" format error.", hostSegment);
        return new JdbdException(m);
    }


}
