package io.jdbd.mysql.protocol.client;

import io.jdbd.JdbdException;
import io.netty.buffer.ByteBuf;

import java.util.function.Consumer;

interface ResultSetReader {


    States read(ByteBuf cumulateBuffer, Consumer<Object> serverStatesConsumer) throws JdbdException;

    enum States {
        MORE_CUMULATE,
        MORE_RESULT,
        MORE_FETCH,
        NO_MORE_RESULT,
        END_ON_ERROR;
    }


}
