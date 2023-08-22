package io.jdbd.mysql.protocol.client;

import io.jdbd.mysql.syntax.MySQLParser;
import io.jdbd.result.ResultStates;
import io.jdbd.session.*;
import io.jdbd.vendor.task.ITaskAdjutant;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.function.Function;

interface TaskAdjutant extends ITaskAdjutant, ClientProtocolAdjutant, MySQLParser {

    int serverStatus();

    boolean isAuthenticated();

    Map<String, Charset> customCharsetMap();

    Map<String, MyCharset> nameCharsetMap();

    Map<Integer, Collation> idCollationMap();

    Map<String, Collation> nameCollationMap();

    ClientProtocolFactory getFactory();







}
