package io.jdbd.mysql.protocol.client;

public class Enums {


    public enum SslMode {
        PREFERRED, REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }

    public enum ZeroDatetimeBehavior { // zeroDateTimeBehavior
        CONVERT_TO_NULL, EXCEPTION, ROUND;
    }

    public enum Compression { // xdevapi.compress
        PREFERRED, REQUIRED, DISABLED;
    }

    public enum XdevapiSslMode {
        REQUIRED, VERIFY_CA, VERIFY_IDENTITY, DISABLED;
    }


}
