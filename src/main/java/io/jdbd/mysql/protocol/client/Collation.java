package io.jdbd.mysql.protocol.client;

public final class Collation {

    public final int index;


    public final String name;
    public final int priority;
    public final MyCharset myCharset;

    Collation(int index, String name, int priority, String charsetName) {
        this(index, name, priority, Charsets.NAME_TO_CHARSET.get(charsetName));
    }

    Collation(int index, String name, int priority, MyCharset myCharset) {
        this.index = index;
        this.name = name;
        this.priority = priority;
        this.myCharset = myCharset;
    }

    public int index() {
        return index;
    }

    public Collation self() {
        return this;
    }


    @Override
    public String toString() {
        StringBuilder asString = new StringBuilder();
        asString.append("[");
        asString.append("index=");
        asString.append(this.index);
        asString.append(",collationName=");
        asString.append(this.name);
        asString.append(",charsetName=");
        asString.append(this.myCharset.name);
        asString.append(",javaCharsetName=");
        asString.append(this.myCharset.getMatchingJavaEncoding(null));
        asString.append("]");
        return asString.toString();
    }


}
