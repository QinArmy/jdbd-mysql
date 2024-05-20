module jdbd.mysql {

    requires jsr305;
    requires java.naming;
    requires java.sql;
    requires org.slf4j;

    requires io.netty.buffer;
    requires io.netty.handler;
    requires io.netty.transport;
    requires io.netty.codec;

    requires org.reactivestreams;
    requires reactor.core;
    requires reactor.netty.core;

    requires jdbd.vendor;

    exports io.jdbd.mysql;
    exports io.jdbd.mysql.env;

}
