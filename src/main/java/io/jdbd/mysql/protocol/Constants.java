package io.jdbd.mysql.protocol;


import java.time.Duration;

/**
 * Represents various constants used in the driver.
 * <p>
 * see {@code com.mysql.cj.Constants}
 * </p>
 */
public interface Constants {


    /**
     * Avoids allocation of empty byte[] when representing 0-length strings.
     */
    byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * I18N'd representation of the abbreviation for "ms"
     */
    String MILLIS_I18N = "ms";

    byte[] SLASH_STAR_SPACE_AS_BYTES = new byte[]{(byte) '/', (byte) '*', (byte) ' '};

    byte[] SPACE_STAR_SLASH_SPACE_AS_BYTES = new byte[]{(byte) ' ', (byte) '*', (byte) '/', (byte) ' '};

    //    String OS_NAME = System.getProperty("");
//    String OS_ARCH = System.getProperty("");
//    String OS_VERSION = System.getProperty("");
//    String PLATFORM_ENCODING = System.getProperty("");


    Duration MAX_DURATION = Duration.ofHours(838)
            .plusMinutes(59)
            .plusSeconds(59)
            .plusMillis(999);

    String NULL = "NULL";

    String TRUE = "TRUE";

    String FALSE = "FALSE";

    String NONE = "none";

    String LOCAL = "LOCAL";

    byte EMPTY_CHAR_BYTE = '\0';

    byte BACK_SLASH_BYTE = '\\';

    byte QUOTE_CHAR_BYTE = '\'';

    byte DOUBLE_QUOTE_BYTE = '"';

    byte PERCENT_BYTE = '%';

    byte UNDERLINE_BYTE = '_';

    String SPACE_SEMICOLON_SPACE = " ; ";

    String SPACE_COMMA_SPACE = " , ";

    char EMPTY_CHAR = '\0';

    char QUOTE = '\'';

    char SPACE = ' ';

    byte SEMICOLON_BYTE = ';';


    //below  Protocol field type numbers, see https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854
    byte TYPE_DECIMAL = 0;
    byte TYPE_TINY = 1;
    byte TYPE_SHORT = 2;
    byte TYPE_LONG = 3;
    byte TYPE_FLOAT = 4;
    byte TYPE_DOUBLE = 5;
    byte TYPE_NULL = 6;
    byte TYPE_TIMESTAMP = 7;
    byte TYPE_LONGLONG = 8;
    byte TYPE_INT24 = 9;
    byte TYPE_DATE = 10;
    byte TYPE_TIME = 11;
    byte TYPE_DATETIME = 12;
    byte TYPE_YEAR = 13;
    byte TYPE_VARCHAR = 15;
    byte TYPE_BIT = 16;
    short TYPE_BOOL = 244;
    short TYPE_JSON = 245;
    short TYPE_ENUM = 247;
    short TYPE_SET = 248;
    short TYPE_TINY_BLOB = 249;
    short TYPE_MEDIUM_BLOB = 250;
    short TYPE_LONG_BLOB = 251;
    short TYPE_BLOB = 252;
    short TYPE_VAR_STRING = 253;
    short TYPE_STRING = 254;
    short TYPE_GEOMETRY = 255;
    short TYPE_NEWDECIMAL = 246;
}
