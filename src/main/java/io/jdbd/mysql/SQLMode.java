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

package io.jdbd.mysql;

/**
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html">Server SQL Modes</a>
 */
public enum SQLMode {

    ALLOW_INVALID_DATES,
    ANSI_QUOTES,
    ERROR_FOR_DIVISION_BY_ZERO,
    HIGH_NOT_PRECEDENCE,

    IGNORE_SPACE,
    NO_AUTO_VALUE_ON_ZERO,
    NO_BACKSLASH_ESCAPES,
    NO_DIR_IN_CREATE,

    NO_ENGINE_SUBSTITUTION,
    NO_UNSIGNED_SUBTRACTION,
    NO_ZERO_DATE,
    NO_ZERO_IN_DATE,

    ONLY_FULL_GROUP_BY,
    PAD_CHAR_TO_FULL_LENGTH,
    PIPES_AS_CONCAT,
    REAL_AS_FLOAT,

    STRICT_ALL_TABLES,
    STRICT_TRANS_TABLES,
    TIME_TRUNCATE_FRACTIONAL

}
