package io.jdbd.mysql.session;

import io.jdbd.mysql.protocol.client.AuthenticationPlugin;
import io.jdbd.mysql.protocol.conf.MyKey;
import io.jdbd.mysql.protocol.conf.MySQLUrl;
import io.jdbd.vendor.session.ISessionAdjutant;

import java.nio.charset.Charset;
import java.util.Map;

public interface SessionAdjutant extends ISessionAdjutant {

    @Override
    MySQLUrl jdbcUrl();

    /**
     * <p>
     * return a enabled plugin name map.
     *     <ul>
     *         <li>key : {@link AuthenticationPlugin#pluginName()}</li>
     *         <li>value : {@link AuthenticationPlugin} class </li>
     *     </ul>
     * </p>
     *
     * @return a unmodifiable map.
     * @see MyKey#defaultAuthenticationPlugin
     * @see MyKey#authenticationPlugins
     * @see MyKey#disabledAuthenticationPlugins
     */
    Map<String, Class<? extends AuthenticationPlugin>> pluginClassMap();

    /**
     * @return a unmodified map
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-connp-props-session.html">customCharsetMapping</a>
     */
    Map<String, Charset> customCharsetMap();

}
