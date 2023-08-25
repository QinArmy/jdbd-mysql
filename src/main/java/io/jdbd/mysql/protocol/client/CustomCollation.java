package io.jdbd.mysql.protocol.client;

final class CustomCollation {

    final int index;

    final String collationName;

    final String charsetName;

    final int maxLen;

    CustomCollation(int index, String collationName, String charsetName, int maxLen) {
        this.index = index;
        this.collationName = collationName;
        this.charsetName = charsetName;
        this.maxLen = maxLen;
    }


}
