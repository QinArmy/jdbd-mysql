package io.jdbd.mysql.util;

import io.jdbd.vendor.util.SQLStates;

public abstract class MySQLStates extends SQLStates {

    protected MySQLStates() {
        throw new UnsupportedOperationException();
    }

    // SQL-92
    public static final String SUCCESSFUL = "00000";


    public static final String WARNING = "01000";
    public static final String DISCONNECT_ERROR = "01002";
    public static final String DATE_TRUNCATED = "01004";
    public static final String PRIVILEGE_NOT_REVOKED = "01006";

    public static final String NO_DATA = "02000";
    public static final String WRONG_NO_OF_PARAMETERS = "07001";
    public static final String UNABLE_TO_CONNECT_TO_DATASOURCE = "08001";
    public static final String CONNECTION_IN_USE = "08002";

    public static final String CONNECTION_NOT_OPEN = "08003";
    public static final String CONNECTION_REJECTED = "08004";
    public static final String CONNECTION_FAILURE = "08006";
    public static final String TRANSACTION_RESOLUTION_UNKNOWN = "08007";

    public static final String COMMUNICATION_LINK_FAILURE = "08S01";
    public static final String FEATURE_NOT_SUPPORTED = "0A000";
    public static final String CARDINALITY_VIOLATION = "21000";
    public static final String INSERT_VALUE_LIST_NO_MATCH_COL_LIST = "21S01";

    public static final String STRING_DATA_RIGHT_TRUNCATION = "22001";
    public static final String NUMERIC_VALUE_OUT_OF_RANGE = "22003";
    public static final String INVALID_DATETIME_FORMAT = "22007";
    public static final String DATETIME_FIELD_OVERFLOW = "22008";

    public static final String DIVISION_BY_ZERO = "22012";
    public static final String INVALID_CHARACTER_VALUE_FOR_CAST = "22018";
    public static final String INTEGRITY_CONSTRAINT_VIOLATION = "23000";
    public static final String INVALID_CURSOR_STATE = "24000";

    public static final String INVALID_TRANSACTION_STATE = "25000";
    public static final String INVALID_AUTH_SPEC = "28000";
    public static final String INVALID_TRANSACTION_TERMINATION = "2D000";
    public static final String INVALID_CONDITION_NUMBER = "35000";

    public static final String INVALID_CATALOG_NAME = "3D000";
    public static final String ROLLBACK_SERIALIZATION_FAILURE = "40001";
    public static final String SYNTAX_ERROR = "42000";
    public static final String ER_TABLE_EXISTS_ERROR = "42S01";

    public static final String BASE_TABLE_OR_VIEW_NOT_FOUND = "42S02";
    public static final String ER_NO_SUCH_INDEX = "42S12";
    public static final String ER_DUP_FIELDNAME = "42S21";
    public static final String ER_BAD_FIELD_ERROR = "42S22";

    // SQL-99
    public static final String INVALID_CONNECTION_ATTRIBUTE = "01S00";
    public static final String ERROR_IN_ROW = "01S01";
    public static final String NO_ROWS_UPDATED_OR_DELETED = "01S03";
    public static final String MORE_THAN_ONE_ROW_UPDATED_OR_DELETED = "01S04";

    public static final String RESIGNAL_WHEN_HANDLER_NOT_ACTIVE = "0K000";
    public static final String STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER = "0Z002";
    public static final String CASE_NOT_FOUND_FOR_CASE_STATEMENT = "20000";
    public static final String NULL_VALUE_NOT_ALLOWED = "22004";

    public static final String INVALID_LOGARITHM_ARGUMENT = "2201E";
    public static final String ACTIVE_SQL_TRANSACTION = "25001";
    public static final String READ_ONLY_SQL_TRANSACTION = "25006";
    public static final String SRE_PROHIBITED_SQL_STATEMENT_ATTEMPTED = "2F003";

    public static final String SRE_FUNCTION_EXECUTED_NO_RETURN_STATEMENT = "2F005";
    public static final String ER_QUERY_INTERRUPTED = "70100";
    public static final String BASE_TABLE_OR_VIEW_ALREADY_EXISTS = "S0001";
    public static final String BASE_TABLE_NOT_FOUND = "S0002";

    public static final String INDEX_ALREADY_EXISTS = "S0011";
    public static final String INDEX_NOT_FOUND = "S0012";
    public static final String COLUMN_ALREADY_EXISTS = "S0021";
    public static final String COLUMN_NOT_FOUND = "S0022";

    public static final String NO_DEFAULT_FOR_COLUMN = "S0023";
    public static final String GENERAL_ERROR = "S1000";
    public static final String MEMORY_ALLOCATION_FAILURE = "S1001";
    public static final String INVALID_COLUMN_NUMBER = "S1002";

    public static final String ILLEGAL_ARGUMENT = "S1009";
    public static final String DRIVER_NOT_CAPABLE = "S1C00";
    public static final String TIMEOUT_EXPIRED = "S1T00";
    public static final String CLI_SPECIFIC_CONDITION = "HY000";

    public static final String MEMORY_ALLOCATION_ERROR = "HY001";

    public static final String ER_WRONG_ARGUMENTS = "HY000";


}
