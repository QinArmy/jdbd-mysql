/*
 * Copyright 2023-2043 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jdbd.mysql.protocol.client;

import java.util.Objects;

public final class Collation {

    public final int index;

    public final String name;
    public final int priority;
    public final MyCharset myCharset;

  //  public final String charsetName;
    public final boolean existsInDatabase;

    Collation(int index, String name, int priority, String charsetName) {
        this(index, name, priority, Charsets.NAME_TO_CHARSET.get(charsetName), true);
    }


    Collation(int index, String name, int priority, String charsetName, boolean existsInDatabase) {
        this(index, name, priority, Charsets.NAME_TO_CHARSET.get(charsetName), existsInDatabase);
    }

    // just for generate correct {@link Collation} code
//    Collation(int index, String name, int priority, boolean trme, String charsetName) {
//        this.index = index;
//        this.name = name;
//        this.priority = priority;
//        this.myCharset = null;
//        this.charsetName = charsetName;
//        this.existsInDatabase = true;
//    }

    Collation(int index, String name, int priority, MyCharset myCharset) {
        this(index, name, priority, myCharset, false);
    }

    private Collation(int index, String name, int priority, MyCharset myCharset, boolean existsInDatabase) {
        Objects.requireNonNull(myCharset);
        this.index = index;
        this.name = name;
        this.priority = priority;
        this.myCharset = myCharset;
        this.existsInDatabase = existsInDatabase;
        //  this.charsetName = myCharset.name;
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
