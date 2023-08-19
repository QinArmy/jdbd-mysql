package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.vendor.task.ITaskAdjutant;

import java.nio.charset.Charset;
import java.util.Map;

interface TaskAdjutant extends ITaskAdjutant, ClientProtocolAdjutant, MySQLParser {

    int serverStatus();

    boolean isAuthenticated();

    Map<String, Charset> customCharsetMap();

    Map<String, MyCharset> nameCharsetMap();

    Map<Integer, Collation> idCollationMap();

    Map<String, Collation> nameCollationMap();

    ClientProtocolFactory getFactory();



}
