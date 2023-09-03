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

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet">max_allowed_packet</a>
     */
    int sessionMaxAllowedPacket();

    ClientProtocolFactory getFactory();

    void addSessionCloseListener(Runnable listener);

    void addTransactionEndListener(Runnable listener);


}
