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

package io.jdbd.mysql.protocol;

import io.jdbd.session.Closeable;
import io.jdbd.session.OptionSpec;
import reactor.core.publisher.Mono;

public interface MySQLProtocolFactory extends OptionSpec, Closeable {

    String factoryName();

    Mono<MySQLProtocol> createProtocol();


    @Override
    <T> Mono<T> close();

    /**
     * override {@link Object#toString()}
     *
     * @return driver info, contain : <ol>
     * <li>implementation class name</li>
     * <li>{@link #factoryName()}</li>
     * <li>{@link System#identityHashCode(Object)}</li>
     * </ol>
     */
    @Override
    String toString();


}
