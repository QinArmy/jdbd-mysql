package io.jdbd.mysql.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html">Mapping MySQL Error Numbers to JDBC SQLState Codes</a>
 */
public abstract class MySQLCodes {

    protected MySQLCodes() {
        throw new UnsupportedOperationException();
    }

    public static final int ER_DUP_KEY = 1022;
    public static final int ER_OUTOFMEMORY = 1037;
    public static final int ER_OUT_OF_SORTMEMORY = 1038;
    public static final int ER_CON_COUNT_ERROR = 1040;

    public static final int ER_BAD_HOST_ERROR = 1042;
    public static final int ER_HANDSHAKE_ERROR = 1043;
    public static final int ER_DBACCESS_DENIED_ERROR = 1044;
    public static final int ER_ACCESS_DENIED_ERROR = 1045;

    public static final int ER_NO_DB_ERROR = 1046;
    public static final int ER_UNKNOWN_COM_ERROR = 1047;
    public static final int ER_BAD_NULL_ERROR = 1048;
    public static final int ER_BAD_DB_ERROR = 1049;

    public static final int ER_TABLE_EXISTS_ERROR = 1050;
    public static final int ER_BAD_TABLE_ERROR = 1051;
    public static final int ER_NON_UNIQ_ERROR = 1052;
    public static final int ER_SERVER_SHUTDOWN = 1053;

    public static final int ER_BAD_FIELD_ERROR = 1054;
    public static final int ER_WRONG_FIELD_WITH_GROUP = 1055;
    public static final int ER_WRONG_GROUP_FIELD = 1056;
    public static final int ER_WRONG_SUM_SELECT = 1057;

    public static final int ER_WRONG_VALUE_COUNT = 1058;
    public static final int ER_TOO_LONG_IDENT = 1059;
    public static final int ER_DUP_FIELDNAME = 1060;
    public static final int ER_DUP_KEYNAME = 1061;

    public static final int ER_DUP_ENTRY = 1062;
    public static final int ER_WRONG_FIELD_SPEC = 1063;
    public static final int ER_PARSE_ERROR = 1064;
    public static final int ER_EMPTY_QUERY = 1065;

    public static final int ER_NONUNIQ_TABLE = 1066;
    public static final int ER_INVALID_DEFAULT = 1067;
    public static final int ER_MULTIPLE_PRI_KEY = 1068;
    public static final int ER_TOO_MANY_KEYS = 1069;

    public static final int ER_TOO_MANY_KEY_PARTS = 1070;
    public static final int ER_TOO_LONG_KEY = 1071;
    public static final int ER_KEY_COLUMN_DOES_NOT_EXITS = 1072;
    public static final int ER_BLOB_USED_AS_KEY = 1073;

    public static final int ER_TOO_BIG_FIELDLENGTH = 1074;
    public static final int ER_WRONG_AUTO_KEY = 1075;
    public static final int ER_FORCING_CLOSE = 1080;
    public static final int ER_IPSOCK_ERROR = 1081;

    public static final int ER_NO_SUCH_INDEX = 1082;
    public static final int ER_WRONG_FIELD_TERMINATORS = 1083;
    public static final int ER_BLOBS_AND_NO_TERMINATED = 1084;
    public static final int ER_CANT_REMOVE_ALL_FIELDS = 1090;

    public static final int ER_CANT_DROP_FIELD_OR_KEY = 1091;
    public static final int ER_BLOB_CANT_HAVE_DEFAULT = 1101;
    public static final int ER_WRONG_DB_NAME = 1102;
    public static final int ER_WRONG_TABLE_NAME = 1103;

    public static final int ER_TOO_BIG_SELECT = 1104;
    public static final int ER_UNKNOWN_PROCEDURE = 1106;
    public static final int ER_WRONG_PARAMCOUNT_TO_PROCEDURE = 1107;
    public static final int ER_UNKNOWN_TABLE = 1109;

    public static final int ER_FIELD_SPECIFIED_TWICE = 1110;
    public static final int ER_UNSUPPORTED_EXTENSION = 1112;
    public static final int ER_TABLE_MUST_HAVE_COLUMNS = 1113;
    public static final int ER_UNKNOWN_CHARACTER_SET = 1115;

    public static final int ER_TOO_BIG_ROWSIZE = 1118;
    public static final int ER_WRONG_OUTER_JOIN = 1120;
    public static final int ER_NULL_COLUMN_IN_INDEX = 1121;
    public static final int ER_PASSWORD_ANONYMOUS_USER = 1131;

    public static final int ER_PASSWORD_NOT_ALLOWED = 1132;
    public static final int ER_PASSWORD_NO_MATCH = 1133;
    public static final int ER_WRONG_VALUE_COUNT_ON_ROW = 1136;
    public static final int ER_INVALID_USE_OF_NULL = 1138;

    public static final int ER_REGEXP_ERROR = 1139;
    public static final int ER_MIX_OF_GROUP_FUNC_AND_FIELDS = 1140;
    public static final int ER_NONEXISTING_GRANT = 1141;
    public static final int ER_TABLEACCESS_DENIED_ERROR = 1142;

    public static final int ER_COLUMNACCESS_DENIED_ERROR = 1143;
    public static final int ER_ILLEGAL_GRANT_FOR_TABLE = 1144;
    public static final int ER_GRANT_WRONG_HOST_OR_USER = 1145;
    public static final int ER_NO_SUCH_TABLE = 1146;

    public static final int ER_NONEXISTING_TABLE_GRANT = 1147;
    public static final int ER_NOT_ALLOWED_COMMAND = 1148;
    public static final int ER_SYNTAX_ERROR = 1149;
    public static final int ER_ABORTING_CONNECTION = 1152;

    public static final int ER_NET_PACKET_TOO_LARGE = 1153;
    public static final int ER_NET_READ_ERROR_FROM_PIPE = 1154;
    public static final int ER_NET_FCNTL_ERROR = 1155;
    public static final int ER_NET_PACKETS_OUT_OF_ORDER = 1156;

    public static final int ER_NET_UNCOMPRESS_ERROR = 1157;
    public static final int ER_NET_READ_ERROR = 1158;
    public static final int ER_NET_READ_INTERRUPTED = 1159;
    public static final int ER_NET_ERROR_ON_WRITE = 1160;

    public static final int ER_NET_WRITE_INTERRUPTED = 1161;
    public static final int ER_TOO_LONG_STRING = 1162;
    public static final int ER_TABLE_CANT_HANDLE_BLOB = 1163;
    public static final int ER_TABLE_CANT_HANDLE_AUTO_INCREMENT = 1164;

    public static final int ER_WRONG_COLUMN_NAME = 1166;
    public static final int ER_WRONG_KEY_COLUMN = 1167;
    public static final int ER_DUP_UNIQUE = 1169;
    public static final int ER_BLOB_KEY_WITHOUT_LENGTH = 1170;

    public static final int ER_PRIMARY_CANT_HAVE_NULL = 1171;
    public static final int ER_TOO_MANY_ROWS = 1172;
    public static final int ER_REQUIRES_PRIMARY_KEY = 1173;
    public static final int ER_KEY_DOES_NOT_EXITS = 1176;

    public static final int ER_CHECK_NO_SUCH_TABLE = 1177;
    public static final int ER_CHECK_NOT_IMPLEMENTED = 1178;
    public static final int ER_CANT_DO_THIS_DURING_AN_TRANSACTION = 1179;
    public static final int ER_NEW_ABORTING_CONNECTION = 1184;

    public static final int ER_SOURCE_NET_READ = 1189;
    public static final int ER_SOURCE_NET_WRITE = 1190;
    public static final int ER_TOO_MANY_USER_CONNECTIONS = 1203;
    public static final int ER_LOCK_WAIT_TIMEOUT = 1205;

    public static final int ER_READ_ONLY_TRANSACTION = 1207;
    public static final int ER_NO_PERMISSION_TO_CREATE_USER = 1211;
    public static final int ER_LOCK_DEADLOCK = 1213;
    public static final int ER_NO_REFERENCED_ROW = 1216;

    public static final int ER_ROW_IS_REFERENCED = 1217;
    public static final int ER_CONNECT_TO_SOURCE = 1218;
    public static final int ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT = 1222;
    public static final int ER_USER_LIMIT_REACHED = 1226;

    public static final int ER_SPECIFIC_ACCESS_DENIED_ERROR = 1227;
    public static final int ER_NO_DEFAULT = 1230;
    public static final int ER_WRONG_VALUE_FOR_VAR = 1231;
    public static final int ER_WRONG_TYPE_FOR_VAR = 1232;

    public static final int ER_CANT_USE_OPTION_HERE = 1234;
    public static final int ER_NOT_SUPPORTED_YET = 1235;
    public static final int ER_WRONG_FK_DEF = 1239;
    public static final int ER_OPERAND_COLUMNS = 1241;

    public static final int ER_SUBQUERY_NO_1_ROW = 1242;
    public static final int ER_ILLEGAL_REFERENCE = 1247;
    public static final int ER_DERIVED_MUST_HAVE_ALIAS = 1248;
    public static final int ER_SELECT_REDUCED = 1249;

    public static final int ER_TABLENAME_NOT_ALLOWED_HERE = 1250;
    public static final int ER_NOT_SUPPORTED_AUTH_MODE = 1251;
    public static final int ER_SPATIAL_CANT_HAVE_NULL = 1252;
    public static final int ER_COLLATION_CHARSET_MISMATCH = 1253;

    public static final int ER_WARN_TOO_FEW_RECORDS = 1261;
    public static final int ER_WARN_TOO_MANY_RECORDS = 1262;
    public static final int ER_WARN_NULL_TO_NOTNULL = 1263;
    public static final int ER_WARN_DATA_OUT_OF_RANGE = 1264;

    public static final int ER_WARN_DATA_TRUNCATED = 1265;
    public static final int ER_WRONG_NAME_FOR_INDEX = 1280;
    public static final int ER_WRONG_NAME_FOR_CATALOG = 1281;
    public static final int ER_UNKNOWN_STORAGE_ENGINE = 1286;

    public static final int ER_TRUNCATED_WRONG_VALUE = 1292;
    public static final int ER_SP_NO_RECURSIVE_CREATE = 1303;
    public static final int ER_SP_ALREADY_EXISTS = 1304;
    public static final int ER_SP_DOES_NOT_EXIST = 1305;

    public static final int ER_SP_LILABEL_MISMATCH = 1308;
    public static final int ER_SP_LABEL_REDEFINE = 1309;
    public static final int ER_SP_LABEL_MISMATCH = 1310;
    public static final int ER_SP_UNINIT_VAR = 1311;

    public static final int ER_SP_BADSELECT = 1312;
    public static final int ER_SP_BADRETURN = 1313;
    public static final int ER_SP_BADSTATEMENT = 1314;
    public static final int ER_UPDATE_LOG_DEPRECATED_IGNORED = 1315;

    public static final int ER_UPDATE_LOG_DEPRECATED_TRANSLATED = 1316;
    public static final int ER_QUERY_INTERRUPTED = 1317;
    public static final int ER_SP_WRONG_NO_OF_ARGS = 1318;
    public static final int ER_SP_COND_MISMATCH = 1319;

    public static final int ER_SP_NORETURN = 1320;
    public static final int ER_SP_NORETURNEND = 1321;
    public static final int ER_SP_BAD_CURSOR_QUERY = 1322;
    public static final int ER_SP_BAD_CURSOR_SELECT = 1323;

    public static final int ER_SP_CURSOR_MISMATCH = 1324;
    public static final int ER_SP_CURSOR_ALREADY_OPEN = 1325;
    public static final int ER_SP_CURSOR_NOT_OPEN = 1326;
    public static final int ER_SP_UNDECLARED_VAR = 1327;

    public static final int ER_SP_FETCH_NO_DATA = 1329;
    public static final int ER_SP_DUP_PARAM = 1330;
    public static final int ER_SP_DUP_VAR = 1331;
    public static final int ER_SP_DUP_COND = 1332;

    public static final int ER_SP_DUP_CURS = 1333;
    public static final int ER_SP_SUBSELECT_NYI = 1335;
    public static final int ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG = 1336;
    public static final int ER_SP_VARCOND_AFTER_CURSHNDLR = 1337;

    public static final int ER_SP_CURSOR_AFTER_HANDLER = 1338;
    public static final int ER_SP_CASE_NOT_FOUND = 1339;
    public static final int ER_DIVISION_BY_ZERO = 1365;
    public static final int ER_ILLEGAL_VALUE_FOR_TYPE = 1367;

    public static final int ER_PROCACCESS_DENIED_ERROR = 1370;
    public static final int ER_XAER_NOTA = 1397;
    public static final int ER_XAER_INVAL = 1398;
    public static final int ER_XAER_RMFAIL = 1399;

    public static final int ER_XAER_OUTSIDE = 1400;
    public static final int ER_XA_RMERR = 1401;
    public static final int ER_XA_RBROLLBACK = 1402;
    public static final int ER_NONEXISTING_PROC_GRANT = 1403;

    public static final int ER_DATA_TOO_LONG = 1406;
    public static final int ER_SP_BAD_SQLSTATE = 1407;
    public static final int ER_CANT_CREATE_USER_WITH_GRANT = 1410;
    public static final int ER_SP_DUP_HANDLER = 1413;

    public static final int ER_SP_NOT_VAR_ARG = 1414;
    public static final int ER_SP_NO_RETSET = 1415;
    public static final int ER_CANT_CREATE_GEOMETRY_OBJECT = 1416;
    public static final int ER_TOO_BIG_SCALE = 1425;

    public static final int ER_TOO_BIG_PRECISION = 1426;
    public static final int ER_M_BIGGER_THAN_D = 1427;
    public static final int ER_TOO_LONG_BODY = 1437;
    public static final int ER_TOO_BIG_DISPLAYWIDTH = 1439;

    public static final int ER_XAER_DUPID = 1440;
    public static final int ER_DATETIME_FUNCTION_OVERFLOW = 1441;
    public static final int ER_ROW_IS_REFERENCED_2 = 1451;
    public static final int ER_NO_REFERENCED_ROW_2 = 1452;

    public static final int ER_SP_BAD_VAR_SHADOW = 1453;
    public static final int ER_SP_WRONG_NAME = 1458;
    public static final int ER_SP_NO_AGGREGATE = 1460;
    public static final int ER_MAX_PREPARED_STMT_COUNT_REACHED = 1461;

    public static final int ER_NON_GROUPING_FIELD_USED = 1463;
    public static final int ER_FOREIGN_DUPLICATE_KEY = 1557;
    public static final int ER_CANT_CHANGE_TX_ISOLATION = 1568;
    public static final int ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT = 1582;

    public static final int ER_WRONG_PARAMETERS_TO_NATIVE_FCT = 1583;
    public static final int ER_WRONG_PARAMETERS_TO_STORED_FCT = 1584;
    public static final int ER_DUP_ENTRY_WITH_KEY_NAME = 1586;
    public static final int ER_XA_RBTIMEOUT = 1613;

    public static final int ER_XA_RBDEADLOCK = 1614;
    public static final int ER_FUNC_INEXISTENT_NAME_COLLISION = 1630;
    public static final int ER_DUP_SIGNAL_SET = 1641;
    public static final int ER_SIGNAL_WARN = 1642;

    public static final int ER_SIGNAL_NOT_FOUND = 1643;
    public static final int ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER = 1645;
    public static final int ER_SPATIAL_MUST_HAVE_GEOM_COL = 1687;
    public static final int ER_DATA_OUT_OF_RANGE = 1690;

    public static final int ER_ACCESS_DENIED_NO_PASSWORD_ERROR = 1698;
    public static final int ER_TRUNCATE_ILLEGAL_FK = 1701;
    public static final int ER_DA_INVALID_CONDITION_NUMBER = 1758;
    public static final int ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO = 1761;

    public static final int ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO = 1762;
    public static final int ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION = 1792;
    public static final int ER_ALTER_OPERATION_NOT_SUPPORTED = 1845;
    public static final int ER_ALTER_OPERATION_NOT_SUPPORTED_REASON = 1846;

    public static final int ER_DUP_UNKNOWN_IN_INDEX = 1859;
    public static final int ER_ACCESS_DENIED_CHANGE_USER_ERROR = 1873;
    public static final int ER_GET_STACKED_DA_WITHOUT_ACTIVE_HANDLER = 1887;
    public static final int ER_INVALID_ARGUMENT_FOR_LOGARITHM = 1903;

    public static final int CR_UNKNOWN_ERROR = 2000;

    public static final int CR_UNSUPPORTED_PARAM_TYPE = 2036;

    public static final int ER_TRUNCATED_WRONG_VALUE_FOR_FIELD = 1366;

    public static final int ER_WRONG_ARGUMENTS = 1210;

    /**
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html">Mapping MySQL Error Numbers to JDBC SQLState Codes</a>
     */
    public static final Map<Integer, String> ERROR_TO_SQL_STATES_MAP = createMySQLErrorNumberToSQLStatesMap();

    /*################################## blow private static method ##################################*/

    /**
     * @return a unmodifiable map
     * @see <a href="https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-error-sqlstates.html">Mapping MySQL Error Numbers to JDBC SQLState Codes</a>
     */
    private static Map<Integer, String> createMySQLErrorNumberToSQLStatesMap() {
        Map<Integer, String> map = new HashMap<>((int) (228 / 0.75F));

        map.put(ER_DUP_KEY, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_OUTOFMEMORY, MySQLStates.MEMORY_ALLOCATION_ERROR);
        map.put(ER_OUT_OF_SORTMEMORY, MySQLStates.MEMORY_ALLOCATION_ERROR);
        map.put(ER_CON_COUNT_ERROR, MySQLStates.CONNECTION_REJECTED);

        map.put(ER_BAD_HOST_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_HANDSHAKE_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_DBACCESS_DENIED_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_ACCESS_DENIED_ERROR, MySQLStates.INVALID_AUTH_SPEC);

        map.put(ER_NO_DB_ERROR, MySQLStates.INVALID_CATALOG_NAME);
        map.put(ER_UNKNOWN_COM_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_BAD_NULL_ERROR, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_BAD_DB_ERROR, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TABLE_EXISTS_ERROR, MySQLStates.ER_TABLE_EXISTS_ERROR);
        map.put(ER_BAD_TABLE_ERROR, MySQLStates.BASE_TABLE_OR_VIEW_NOT_FOUND);
        map.put(ER_NON_UNIQ_ERROR, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_SERVER_SHUTDOWN, MySQLStates.COMMUNICATION_LINK_FAILURE);

        map.put(ER_BAD_FIELD_ERROR, MySQLStates.ER_BAD_FIELD_ERROR);
        map.put(ER_WRONG_FIELD_WITH_GROUP, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_GROUP_FIELD, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_SUM_SELECT, MySQLStates.SYNTAX_ERROR);

        map.put(ER_WRONG_VALUE_COUNT, MySQLStates.INSERT_VALUE_LIST_NO_MATCH_COL_LIST);
        map.put(ER_TOO_LONG_IDENT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_DUP_FIELDNAME, MySQLStates.ER_DUP_FIELDNAME);
        map.put(ER_DUP_KEYNAME, MySQLStates.SYNTAX_ERROR);

        map.put(ER_DUP_ENTRY, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_WRONG_FIELD_SPEC, MySQLStates.SYNTAX_ERROR);
        map.put(ER_PARSE_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_EMPTY_QUERY, MySQLStates.SYNTAX_ERROR);

        map.put(ER_NONUNIQ_TABLE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_INVALID_DEFAULT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_MULTIPLE_PRI_KEY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TOO_MANY_KEYS, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TOO_MANY_KEY_PARTS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TOO_LONG_KEY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_KEY_COLUMN_DOES_NOT_EXITS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_BLOB_USED_AS_KEY, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TOO_BIG_FIELDLENGTH, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_AUTO_KEY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_FORCING_CLOSE, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_IPSOCK_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);

        map.put(ER_NO_SUCH_INDEX, MySQLStates.ER_NO_SUCH_INDEX);
        map.put(ER_WRONG_FIELD_TERMINATORS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_BLOBS_AND_NO_TERMINATED, MySQLStates.SYNTAX_ERROR);
        map.put(ER_CANT_REMOVE_ALL_FIELDS, MySQLStates.SYNTAX_ERROR);

        map.put(ER_CANT_DROP_FIELD_OR_KEY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_BLOB_CANT_HAVE_DEFAULT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_DB_NAME, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_TABLE_NAME, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TOO_BIG_SELECT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_UNKNOWN_PROCEDURE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_PARAMCOUNT_TO_PROCEDURE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_UNKNOWN_TABLE, MySQLStates.BASE_TABLE_OR_VIEW_NOT_FOUND);

        map.put(ER_FIELD_SPECIFIED_TWICE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_UNSUPPORTED_EXTENSION, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TABLE_MUST_HAVE_COLUMNS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_UNKNOWN_CHARACTER_SET, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TOO_BIG_ROWSIZE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_OUTER_JOIN, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NULL_COLUMN_IN_INDEX, MySQLStates.SYNTAX_ERROR);
        map.put(ER_PASSWORD_ANONYMOUS_USER, MySQLStates.SYNTAX_ERROR);

        map.put(ER_PASSWORD_NOT_ALLOWED, MySQLStates.SYNTAX_ERROR);
        map.put(ER_PASSWORD_NO_MATCH, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_VALUE_COUNT_ON_ROW, MySQLStates.INSERT_VALUE_LIST_NO_MATCH_COL_LIST);
        map.put(ER_INVALID_USE_OF_NULL, MySQLStates.NULL_VALUE_NOT_ALLOWED);

        map.put(ER_REGEXP_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_MIX_OF_GROUP_FUNC_AND_FIELDS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NONEXISTING_GRANT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TABLEACCESS_DENIED_ERROR, MySQLStates.SYNTAX_ERROR);

        map.put(ER_COLUMNACCESS_DENIED_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_ILLEGAL_GRANT_FOR_TABLE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_GRANT_WRONG_HOST_OR_USER, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NO_SUCH_TABLE, MySQLStates.BASE_TABLE_OR_VIEW_NOT_FOUND);

        map.put(ER_NONEXISTING_TABLE_GRANT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NOT_ALLOWED_COMMAND, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SYNTAX_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_ABORTING_CONNECTION, MySQLStates.COMMUNICATION_LINK_FAILURE);

        map.put(ER_NET_PACKET_TOO_LARGE, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_NET_READ_ERROR_FROM_PIPE, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_NET_FCNTL_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_NET_PACKETS_OUT_OF_ORDER, MySQLStates.COMMUNICATION_LINK_FAILURE);

        map.put(ER_NET_UNCOMPRESS_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_NET_READ_ERROR, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_NET_READ_INTERRUPTED, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_NET_ERROR_ON_WRITE, MySQLStates.COMMUNICATION_LINK_FAILURE);

        map.put(ER_NET_WRITE_INTERRUPTED, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_TOO_LONG_STRING, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TABLE_CANT_HANDLE_BLOB, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TABLE_CANT_HANDLE_AUTO_INCREMENT, MySQLStates.SYNTAX_ERROR);

        map.put(ER_WRONG_COLUMN_NAME, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_KEY_COLUMN, MySQLStates.SYNTAX_ERROR);
        map.put(ER_DUP_UNIQUE, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_BLOB_KEY_WITHOUT_LENGTH, MySQLStates.SYNTAX_ERROR);

        map.put(ER_PRIMARY_CANT_HAVE_NULL, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TOO_MANY_ROWS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_REQUIRES_PRIMARY_KEY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_KEY_DOES_NOT_EXITS, MySQLStates.SYNTAX_ERROR);

        map.put(ER_CHECK_NO_SUCH_TABLE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_CHECK_NOT_IMPLEMENTED, MySQLStates.SYNTAX_ERROR);
        map.put(ER_CANT_DO_THIS_DURING_AN_TRANSACTION, MySQLStates.INVALID_TRANSACTION_STATE);
        map.put(ER_NEW_ABORTING_CONNECTION, MySQLStates.COMMUNICATION_LINK_FAILURE);

        map.put(ER_SOURCE_NET_READ, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_SOURCE_NET_WRITE, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_TOO_MANY_USER_CONNECTIONS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_LOCK_WAIT_TIMEOUT, MySQLStates.ROLLBACK_SERIALIZATION_FAILURE);

        map.put(ER_READ_ONLY_TRANSACTION, MySQLStates.INVALID_TRANSACTION_STATE);
        map.put(ER_NO_PERMISSION_TO_CREATE_USER, MySQLStates.SYNTAX_ERROR);
        map.put(ER_LOCK_DEADLOCK, MySQLStates.ROLLBACK_SERIALIZATION_FAILURE);
        map.put(ER_NO_REFERENCED_ROW, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);

        map.put(ER_ROW_IS_REFERENCED, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_CONNECT_TO_SOURCE, MySQLStates.COMMUNICATION_LINK_FAILURE);
        map.put(ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, MySQLStates.CARDINALITY_VIOLATION);
        map.put(ER_USER_LIMIT_REACHED, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SPECIFIC_ACCESS_DENIED_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NO_DEFAULT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_VALUE_FOR_VAR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_TYPE_FOR_VAR, MySQLStates.SYNTAX_ERROR);

        map.put(ER_CANT_USE_OPTION_HERE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NOT_SUPPORTED_YET, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_FK_DEF, MySQLStates.SYNTAX_ERROR);
        map.put(ER_OPERAND_COLUMNS, MySQLStates.CARDINALITY_VIOLATION);

        map.put(ER_SUBQUERY_NO_1_ROW, MySQLStates.CARDINALITY_VIOLATION);
        map.put(ER_ILLEGAL_REFERENCE, MySQLStates.ER_BAD_FIELD_ERROR);
        map.put(ER_DERIVED_MUST_HAVE_ALIAS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SELECT_REDUCED, MySQLStates.WARNING);

        map.put(ER_TABLENAME_NOT_ALLOWED_HERE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_NOT_SUPPORTED_AUTH_MODE, MySQLStates.CONNECTION_REJECTED);
        map.put(ER_SPATIAL_CANT_HAVE_NULL, MySQLStates.SYNTAX_ERROR);
        map.put(ER_COLLATION_CHARSET_MISMATCH, MySQLStates.SYNTAX_ERROR);

        map.put(ER_WARN_TOO_FEW_RECORDS, MySQLStates.WARNING);
        map.put(ER_WARN_TOO_MANY_RECORDS, MySQLStates.WARNING);
        map.put(ER_WARN_NULL_TO_NOTNULL, MySQLStates.NULL_VALUE_NOT_ALLOWED);
        map.put(ER_WARN_DATA_OUT_OF_RANGE, MySQLStates.NUMERIC_VALUE_OUT_OF_RANGE);

        map.put(ER_WARN_DATA_TRUNCATED, MySQLStates.WARNING);
        map.put(ER_WRONG_NAME_FOR_INDEX, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_NAME_FOR_CATALOG, MySQLStates.SYNTAX_ERROR);
        map.put(ER_UNKNOWN_STORAGE_ENGINE, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TRUNCATED_WRONG_VALUE, MySQLStates.INVALID_DATETIME_FORMAT);
        map.put(ER_SP_NO_RECURSIVE_CREATE, MySQLStates.SRE_PROHIBITED_SQL_STATEMENT_ATTEMPTED);
        map.put(ER_SP_ALREADY_EXISTS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_DOES_NOT_EXIST, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_LILABEL_MISMATCH, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_LABEL_REDEFINE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_LABEL_MISMATCH, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_UNINIT_VAR, MySQLStates.WARNING);

        map.put(ER_SP_BADSELECT, MySQLStates.FEATURE_NOT_SUPPORTED);
        map.put(ER_SP_BADRETURN, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_BADSTATEMENT, MySQLStates.FEATURE_NOT_SUPPORTED);
        map.put(ER_UPDATE_LOG_DEPRECATED_IGNORED, MySQLStates.SYNTAX_ERROR);

        map.put(ER_UPDATE_LOG_DEPRECATED_TRANSLATED, MySQLStates.SYNTAX_ERROR);
        map.put(ER_QUERY_INTERRUPTED, MySQLStates.ER_QUERY_INTERRUPTED);
        map.put(ER_SP_WRONG_NO_OF_ARGS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_COND_MISMATCH, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_NORETURN, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_NORETURNEND, MySQLStates.SRE_FUNCTION_EXECUTED_NO_RETURN_STATEMENT);
        map.put(ER_SP_BAD_CURSOR_QUERY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_BAD_CURSOR_SELECT, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_CURSOR_MISMATCH, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_CURSOR_ALREADY_OPEN, MySQLStates.INVALID_CURSOR_STATE);
        map.put(ER_SP_CURSOR_NOT_OPEN, MySQLStates.INVALID_CURSOR_STATE);
        map.put(ER_SP_UNDECLARED_VAR, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_FETCH_NO_DATA, MySQLStates.NO_DATA);
        map.put(ER_SP_DUP_PARAM, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_DUP_VAR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_DUP_COND, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_DUP_CURS, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_SUBSELECT_NYI, MySQLStates.FEATURE_NOT_SUPPORTED);
        map.put(ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG, MySQLStates.FEATURE_NOT_SUPPORTED);
        map.put(ER_SP_VARCOND_AFTER_CURSHNDLR, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_CURSOR_AFTER_HANDLER, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_CASE_NOT_FOUND, MySQLStates.CASE_NOT_FOUND_FOR_CASE_STATEMENT);
        map.put(ER_DIVISION_BY_ZERO, MySQLStates.DIVISION_BY_ZERO);
        map.put(ER_ILLEGAL_VALUE_FOR_TYPE, MySQLStates.INVALID_DATETIME_FORMAT);

        map.put(ER_PROCACCESS_DENIED_ERROR, MySQLStates.SYNTAX_ERROR);
        map.put(ER_XAER_NOTA, MySQLStates.ER_XAER_NOTA);
        map.put(ER_XAER_INVAL, MySQLStates.ER_XAER_INVAL);
        map.put(ER_XAER_RMFAIL, MySQLStates.ER_XAER_RMFAIL);

        map.put(ER_XAER_OUTSIDE, MySQLStates.ER_XAER_OUTSIDE);
        map.put(ER_XA_RMERR, MySQLStates.ER_XA_RMERR);
        map.put(ER_XA_RBROLLBACK, MySQLStates.ER_XA_RBROLLBACK);
        map.put(ER_NONEXISTING_PROC_GRANT, MySQLStates.SYNTAX_ERROR);

        map.put(ER_DATA_TOO_LONG, MySQLStates.STRING_DATA_RIGHT_TRUNCATION);
        map.put(ER_SP_BAD_SQLSTATE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_CANT_CREATE_USER_WITH_GRANT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_DUP_HANDLER, MySQLStates.SYNTAX_ERROR);

        map.put(ER_SP_NOT_VAR_ARG, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_NO_RETSET, MySQLStates.FEATURE_NOT_SUPPORTED);
        map.put(ER_CANT_CREATE_GEOMETRY_OBJECT, MySQLStates.NUMERIC_VALUE_OUT_OF_RANGE);
        map.put(ER_TOO_BIG_SCALE, MySQLStates.SYNTAX_ERROR);

        map.put(ER_TOO_BIG_PRECISION, MySQLStates.SYNTAX_ERROR);
        map.put(ER_M_BIGGER_THAN_D, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TOO_LONG_BODY, MySQLStates.SYNTAX_ERROR);
        map.put(ER_TOO_BIG_DISPLAYWIDTH, MySQLStates.SYNTAX_ERROR);

        map.put(ER_XAER_DUPID, MySQLStates.ER_XAER_DUPID);
        map.put(ER_DATETIME_FUNCTION_OVERFLOW, MySQLStates.DATETIME_FIELD_OVERFLOW);
        map.put(ER_ROW_IS_REFERENCED_2, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_NO_REFERENCED_ROW_2, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);

        map.put(ER_SP_BAD_VAR_SHADOW, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_WRONG_NAME, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SP_NO_AGGREGATE, MySQLStates.SYNTAX_ERROR);
        map.put(ER_MAX_PREPARED_STMT_COUNT_REACHED, MySQLStates.SYNTAX_ERROR);

        map.put(ER_NON_GROUPING_FIELD_USED, MySQLStates.SYNTAX_ERROR);
        map.put(ER_FOREIGN_DUPLICATE_KEY, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_CANT_CHANGE_TX_ISOLATION, MySQLStates.ACTIVE_SQL_TRANSACTION);
        map.put(ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT, MySQLStates.SYNTAX_ERROR);

        map.put(ER_WRONG_PARAMETERS_TO_NATIVE_FCT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_WRONG_PARAMETERS_TO_STORED_FCT, MySQLStates.SYNTAX_ERROR);
        map.put(ER_DUP_ENTRY_WITH_KEY_NAME, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_XA_RBTIMEOUT, MySQLStates.XA_RBTIMEOUT);

        map.put(ER_XA_RBDEADLOCK, MySQLStates.ER_XA_RBDEADLOCK);
        map.put(ER_FUNC_INEXISTENT_NAME_COLLISION, MySQLStates.SYNTAX_ERROR);
        map.put(ER_DUP_SIGNAL_SET, MySQLStates.SYNTAX_ERROR);
        map.put(ER_SIGNAL_WARN, MySQLStates.WARNING);

        map.put(ER_SIGNAL_NOT_FOUND, MySQLStates.NO_DATA);
        map.put(ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER, MySQLStates.RESIGNAL_WHEN_HANDLER_NOT_ACTIVE);
        map.put(ER_SPATIAL_MUST_HAVE_GEOM_COL, MySQLStates.SYNTAX_ERROR);
        map.put(ER_DATA_OUT_OF_RANGE, MySQLStates.NUMERIC_VALUE_OUT_OF_RANGE);

        map.put(ER_ACCESS_DENIED_NO_PASSWORD_ERROR, MySQLStates.INVALID_AUTH_SPEC);
        map.put(ER_TRUNCATE_ILLEGAL_FK, MySQLStates.SYNTAX_ERROR);
        map.put(ER_DA_INVALID_CONDITION_NUMBER, MySQLStates.INVALID_CONDITION_NUMBER);
        map.put(ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);

        map.put(ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION, MySQLStates.READ_ONLY_SQL_TRANSACTION);
        map.put(ER_ALTER_OPERATION_NOT_SUPPORTED, MySQLStates.FEATURE_NOT_SUPPORTED);
        map.put(ER_ALTER_OPERATION_NOT_SUPPORTED_REASON, MySQLStates.FEATURE_NOT_SUPPORTED);

        map.put(ER_DUP_UNKNOWN_IN_INDEX, MySQLStates.INTEGRITY_CONSTRAINT_VIOLATION);
        map.put(ER_ACCESS_DENIED_CHANGE_USER_ERROR, MySQLStates.INVALID_AUTH_SPEC);
        map.put(ER_GET_STACKED_DA_WITHOUT_ACTIVE_HANDLER, MySQLStates.STACKED_DIAGNOSTICS_ACCESSED_WITHOUT_ACTIVE_HANDLER);
        map.put(ER_INVALID_ARGUMENT_FOR_LOGARITHM, MySQLStates.INVALID_LOGARITHM_ARGUMENT);

        map.put(ER_TRUNCATED_WRONG_VALUE_FOR_FIELD, MySQLStates.ER_WRONG_ARGUMENTS);
        map.put(ER_WRONG_ARGUMENTS, MySQLStates.ER_WRONG_ARGUMENTS);
        return Collections.unmodifiableMap(map);
    }


}
