package io.jdbd.mysql.protocol.client;

abstract class Capabilities {

    Capabilities() {
        throw new UnsupportedOperationException();
    }

    static boolean supportMultiStatement(final int negotiatedCapability) {
        return (negotiatedCapability & ClientProtocol.CLIENT_MULTI_STATEMENTS) != 0;
    }

    static boolean supportMultiStatement(final ClientProtocolAdjutant adjutant) {
        return supportMultiStatement(adjutant.obtainNegotiatedCapability());
    }

}