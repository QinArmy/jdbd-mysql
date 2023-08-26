package io.jdbd.mysql.protocol;

import io.jdbd.mysql.env.MySQLHostInfo;
import io.netty.buffer.ByteBufAllocator;

import java.nio.charset.Charset;

public interface AuthenticateAssistant {

    Charset getHandshakeCharset();

    Charset getPasswordCharset();

    MySQLHostInfo getHostInfo();

    boolean isUseSsl();


    MySQLServerVersion getServerVersion();

    ByteBufAllocator allocator();

}
