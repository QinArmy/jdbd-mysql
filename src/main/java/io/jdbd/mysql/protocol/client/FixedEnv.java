package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.jdbd.mysql.env.MySQLKey;
import io.jdbd.mysql.protocol.AuthenticateAssistant;
import io.jdbd.mysql.util.MySQLCollections;
import io.jdbd.mysql.util.MySQLExceptions;
import io.jdbd.mysql.util.MySQLStrings;
import io.jdbd.vendor.env.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * <p>
 * This class is base class of {@link ClientProtocolFactory}.
 * </p>
 *
 * @since 1.0
 */
abstract class FixedEnv {

    private static final Logger LOG = LoggerFactory.getLogger(FixedEnv.class);

    private static final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> BUILDIN_PLUGIN_MAP = createPluginConstructorMap();

    final boolean transformedBitIsBoolean;

    final boolean functionsNeverReturnBlobs;

    final boolean blobsAreStrings;

    final int bigColumnBoundaryBytes;

    final int maxAllowedPacket;

    final boolean sendFractionalSeconds;

    final boolean sendFractionalSecondsForTime;

    final int blobSendChunkSize;

    final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> pluginFuncMap;

    final Map<String, Charset> customCharsetMap;

    final Environment env;

    FixedEnv(Environment env) {
        this.transformedBitIsBoolean = env.getOrDefault(MySQLKey.TRANS_FORMED_BIT_IS_BOOLEAN);
        this.functionsNeverReturnBlobs = env.getOrDefault(MySQLKey.FUNCTIONS_NEVER_RETURN_BLOBS);
        this.blobsAreStrings = env.getOrDefault(MySQLKey.BLOBS_ARE_STRINGS);
        this.maxAllowedPacket = parseMaxAllowedPacket(env);

        this.bigColumnBoundaryBytes = env.getInRange(MySQLKey.BIG_COLUMN_BOUNDARY_BYTES, Packets.MAX_PAYLOAD, 1 << 27);
        this.sendFractionalSeconds = env.getOrDefault(MySQLKey.SEND_FRACTIONAL_SECONDS);
        this.sendFractionalSecondsForTime = env.getOrDefault(MySQLKey.SEND_FRACTIONAL_SECONDS_FOR_TIME);
        this.blobSendChunkSize = env.getInRange(MySQLKey.BLOB_SEND_CHUNK_SIZE, 1024, this.maxAllowedPacket - Packets.HEADER_SIZE);

        this.pluginFuncMap = createPluginFuncMap(env);
        this.customCharsetMap = createCustomCharsetMap(env);
        this.env = env;
    }


    /**
     * @see MySQLKey#MAX_ALLOWED_PACKET
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    private static int parseMaxAllowedPacket(final Environment env) {
        final int maxAllowedPacket;
        maxAllowedPacket = env.getOrDefault(MySQLKey.MAX_ALLOWED_PACKET);

        final int defaultValue = MySQLKey.MAX_ALLOWED_PACKET.defaultValue, minValue = 1024, maxValue = 1 << 30;

        final int value;
        if (maxAllowedPacket < 0 || maxAllowedPacket == defaultValue) {
            // (1 << 26) is default value , 64MB
            value = defaultValue;
        } else if (maxAllowedPacket <= minValue) {
            value = minValue;
        } else if (maxAllowedPacket >= maxValue) {
            value = maxValue; //1GB
        } else {
            value = maxAllowedPacket & (~1023); // The value should be a multiple of 1024 .
        }
        return value;
    }


    /**
     * @return a unmodified map
     * @throws io.jdbd.JdbdException when {@link MySQLKey#CUSTOM_CHARSET_MAPPING} value error.
     */
    private static Map<String, Charset> createCustomCharsetMap(final Environment env) {
        final String mappingValue;
        mappingValue = env.get(MySQLKey.CUSTOM_CHARSET_MAPPING);

        if (!MySQLStrings.hasText(mappingValue)) {
            return Collections.emptyMap();
        }
        final String[] pairs;
        pairs = mappingValue.split(",");


        try {
            String[] valuePair;
            final Map<String, Charset> tempMap = MySQLCollections.hashMap((int) (pairs.length / 0.75F));
            for (String pair : pairs) {
                valuePair = pair.split(":");
                if (valuePair.length != 2) {
                    String m = String.format("%s value format error.", MySQLKey.CUSTOM_CHARSET_MAPPING);
                    throw new JdbdException(m);
                }
                tempMap.put(valuePair[0].trim(), Charset.forName(valuePair[1].trim()));
            }
            return MySQLCollections.unmodifiableMap(tempMap);
        } catch (JdbdException e) {
            throw e;
        } catch (Exception e) {
            throw MySQLExceptions.wrap(e);
        }
    }


    /**
     * @return a unmodifiable map ,key : {@link AuthenticationPlugin#pluginName()}.
     * @throws JdbdException throw when below key error:<ul>
     *                       <li>{@link MySQLKey#DEFAULT_AUTHENTICATION_PLUGIN}</li>
     *                       <li>{@link MySQLKey#AUTHENTICATION_PLUGINS}</li>
     *                       <li>{@link MySQLKey#DISABLED_AUTHENTICATION_PLUGINS}</li>
     *                       </ul>
     */
    private static Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> createPluginFuncMap(final Environment env)
            throws JdbdException {

        final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> disabledMap, enabledMap;
        disabledMap = loadPluginMap(env, MySQLKey.DISABLED_AUTHENTICATION_PLUGINS);
        enabledMap = loadPluginMap(env, MySQLKey.AUTHENTICATION_PLUGINS);

        final String defaultPluginName;
        defaultPluginName = env.getOrDefault(MySQLKey.DEFAULT_AUTHENTICATION_PLUGIN);

        if (disabledMap.isEmpty()
                && enabledMap.isEmpty()
                && BUILDIN_PLUGIN_MAP.containsKey(defaultPluginName)) {
            return BUILDIN_PLUGIN_MAP;
        }

        final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> pluginMap;
        pluginMap = MySQLCollections.hashMap(BUILDIN_PLUGIN_MAP);

        for (Map.Entry<String, Function<AuthenticateAssistant, AuthenticationPlugin>> e : enabledMap.entrySet()) {
            if (pluginMap.containsKey(e.getKey())) {
                continue;
            }
            pluginMap.put(e.getKey(), e.getValue());
        }

        for (String s : disabledMap.keySet()) {
            pluginMap.remove(s);
        }

        if (disabledMap.containsKey(defaultPluginName)) {
            String message = String.format("%s[%s] disable.", MySQLKey.DISABLED_AUTHENTICATION_PLUGINS,
                    defaultPluginName);
            throw new JdbdException(message);
        } else if (!pluginMap.containsKey(defaultPluginName)) {
            String message = String.format("%s[%s] not fond.", MySQLKey.DISABLED_AUTHENTICATION_PLUGINS,
                    defaultPluginName);
            throw new JdbdException(message);
        }

        if (LOG.isTraceEnabled()) {
            int index = 0;
            StringBuilder builder = new StringBuilder("enabled authentication mechanisms:\n");
            for (String mechanism : pluginMap.keySet()) {
                index++;
                builder.append(index)
                        .append(" - ")
                        .append(mechanism)
                        .append("\n");
            }
            LOG.trace(builder.toString());
        }
        return MySQLCollections.unmodifiableMap(pluginMap);
    }


    private static Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> createPluginConstructorMap() {
        final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> map;
        map = MySQLCollections.hashMap((int) (5 / 0.75F));

        map.put(MySQLNativePasswordPlugin.PLUGIN_NAME, MySQLNativePasswordPlugin::getInstance);
        map.put(CachingSha2PasswordPlugin.PLUGIN_NAME, CachingSha2PasswordPlugin::getInstance);
        map.put(MySQLClearPasswordPlugin.PLUGIN_NAME, MySQLClearPasswordPlugin::getInstance);
        map.put(MySQLOldPasswordPlugin.PLUGIN_NAME, MySQLOldPasswordPlugin::getInstance);

        map.put(Sha256PasswordPlugin.PLUGIN_NAME, Sha256PasswordPlugin::getInstance);

        return Collections.unmodifiableMap(map);
    }


    private static Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> loadPluginMap(
            final Environment env, final MySQLKey<String> key) throws JdbdException {
        String string = env.get(key);
        if (!MySQLStrings.hasText(string)) {
            return Collections.emptyMap();
        }

        final Set<String> nameSet;
        nameSet = MySQLStrings.spitAsSet(string, ",", true);

        final Map<String, Function<AuthenticateAssistant, AuthenticationPlugin>> map;
        map = MySQLCollections.hashMap();

        Function<AuthenticateAssistant, AuthenticationPlugin> constructor;
        for (String name : nameSet) {
            constructor = BUILDIN_PLUGIN_MAP.get(name);
            if (constructor == null) {
                constructor = loadConstructor(name);
            }
            map.put(name, constructor);
        }

        return MySQLCollections.unmodifiableMap(map);
    }


    private static Function<AuthenticateAssistant, AuthenticationPlugin> loadConstructor(final String name) {
        final int colonIndex;
        colonIndex = name.lastIndexOf("::");
        if (colonIndex < 2 || (colonIndex + 2) >= name.length()) {
            String m = String.format("%s isn't plugin factory method", name);
            throw new JdbdException(m);
        }

        try {
            final Class<?> funcClass;
            funcClass = Class.forName(name.substring(0, colonIndex));

            final Method method;
            method = funcClass.getMethod(name.substring(colonIndex + 2), AuthenticateAssistant.class);

            final int modifier;
            modifier = method.getModifiers();

            final boolean match;
            match = Modifier.isPublic(modifier)
                    && Modifier.isStatic(modifier)
                    && AuthenticationPlugin.class.isAssignableFrom(method.getReturnType());

            if (!match) {
                String m = String.format("%s isn't public static factory method.", name);
                throw new JdbdException(m);
            }
            return createFunction(method);
        } catch (JdbdException e) {
            throw e;
        } catch (Throwable e) {
            String m = String.format("load plugin[%s] factory method occur error.", name);
            throw new JdbdException(m);
        }

    }


    private static Function<AuthenticateAssistant, AuthenticationPlugin> createFunction(final Method method) {
        return assistant -> {
            try {
                final AuthenticationPlugin plugin;
                plugin = (AuthenticationPlugin) method.invoke(null, assistant);

                if (plugin == null) {
                    String m = String.format("plugin factory method %s return null", method.getName());
                    throw new JdbdException(m);
                }
                return plugin;
            } catch (Throwable e) {
                throw new JdbdException("invoke plugin factory method occur error.", e);
            }

        };
    }


}
