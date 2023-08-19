package io.jdbd.mysql.protocol.client;

final class AuthenticateResult {

     final Handshake10 handshake10;

    final int capability;

    AuthenticateResult(Handshake10 handshake10, int capability) {
        this.handshake10 = handshake10;
        this.capability = capability;
    }

    public Handshake10 handshakeV10Packet() {
        return handshake10;
    }

    public int capability() {
        return capability;
    }


}
