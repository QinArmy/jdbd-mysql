package io.jdbd.postgre.syntax;

import io.jdbd.postgre.ServerParameter;
import io.jdbd.postgre.stmt.BatchBindStmt;
import io.jdbd.postgre.stmt.BindValue;
import io.jdbd.postgre.util.PgExceptions;
import io.jdbd.postgre.util.PgStrings;
import io.jdbd.vendor.stmt.ParamStmt;
import io.jdbd.vendor.stmt.ParamValue;
import io.jdbd.vendor.stmt.Stmt;
import org.qinarmy.util.FastStack;
import org.qinarmy.util.Stack;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

final class DefaultPgParser implements PgParser {

    static DefaultPgParser create(Function<ServerParameter, String> paramFunction) {
        return new DefaultPgParser(paramFunction);
    }

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPgParser.class);

    private static final String BLOCK_COMMENT_START_MARKER = "/*";

    private static final String BLOCK_COMMENT_END_MARKER = "*/";

    private static final String DOUBLE_DASH_COMMENT_MARKER = "--";

    private static final String COPY = "COPY";

    private static final String FROM = "FROM";

    private static final String PROGRAM = "PROGRAM";

    private static final String STDIN = "STDIN";

    private static final char QUOTE = '\'';

    private static final char DOUBLE_QUOTE = '"';

    private static final char BACK_SLASH = '\\';

    private static final char SLASH = '/';

    private static final char STAR = '*';

    private static final char DASH = '-';

    private static final char DOLLAR = '$';

    private final Function<ServerParameter, String> paramFunction;


    private DefaultPgParser(Function<ServerParameter, String> paramFunction) {
        this.paramFunction = paramFunction;
    }

    @Override
    public final PgStatement parse(final String sql) throws SQLException {
        return (PgStatement) doParse(sql, true);
    }

    @Override
    public final CopyIn parseCopyIn(final Stmt stmt, int stmtIndex) throws SQLException {
        final String sql = stmt.getSql();
        final char[] charArray = sql.toCharArray();
        final int lastIndex = charArray.length - 1;
        char ch;
        boolean copyCommand = false, fromClause = false;
        CopyIn copyIn = null;
        for (int i = 0; i < charArray.length; i++) {
            ch = charArray[i];
            if (Character.isWhitespace(ch)) {
                continue;
            }
            if (ch == SLASH && i < lastIndex && charArray[i + 1] == STAR) {
                // block comment.
                i = skipBlockComment(sql, i);
            } else if (ch == DASH && i < lastIndex && charArray[i + 1] == DASH) {
                // line comment
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : charArray.length;
            } else if (!copyCommand) {
                if (i == lastIndex
                        || !sql.regionMatches(true, i, COPY, 0, COPY.length())
                        || !Character.isWhitespace(sql.charAt(i + COPY.length()))) {
                    throw PgExceptions.createSyntaxError("Not COPY command.");
                }
                i += COPY.length();
                copyCommand = true;
            } else if (ch == DOUBLE_QUOTE) {
                if (i < lastIndex) {
                    i = sql.indexOf(DOUBLE_QUOTE, i + 1);
                }
                if (i < 0) {
                    throw createDoubleQuotedIdentifierError(sql, i);
                }
            } else if (ch == '?' && !fromClause) {
                String m = String.format(
                        "syntax error,position of placeholder of parameter,at near %s", fragment(sql, i));
                throw PgExceptions.createSyntaxError(m);
            } else if (!fromClause) {
                if ((ch == 'f' || ch == 'F')
                        && Character.isWhitespace(charArray[i - 1])
                        && (i + FROM.length()) < lastIndex
                        && Character.isWhitespace(charArray[i + FROM.length()])
                        && sql.regionMatches(true, i, FROM, 0, FROM.length())) {
                    i += FROM.length();
                    fromClause = true;
                }
            } else if (ch == '?') {
                final String fileName = extractStringConstantFromParamValue(stmt, stmtIndex, i);
                copyIn = new CopyInFromPath(Paths.get(fileName));
                break;
            } else if (i + 3 >= lastIndex) {
                String m = String.format(
                        "syntax error,FROM clause error,at near %s", fragment(sql, i));
                throw PgExceptions.createSyntaxError(m);
            } else if ((ch == 'p' || ch == 'P')
                    && sql.regionMatches(true, i, PROGRAM, 0, PROGRAM.length())
                    && Character.isWhitespace(charArray[i + PROGRAM.length()])) {
                // PROGRAM 'command'
                throw new SQLException("COPY FORM PROGRAM 'command' not supported by jdbd-postgre");
            } else if ((ch == 's' || ch == 'S')
                    && sql.regionMatches(true, i, STDIN, 0, STDIN.length())
                    && (i + STDIN.length() == lastIndex || Character.isWhitespace(charArray[i + STDIN.length()]))) {
                // STDIN
                throw new SQLException("COPY FORM STDIN not supported by jdbd-postgre");
            } else { // 'filename'
                try {
                    final String fileName;
                    fileName = parseStringConstant(stmt, charArray, stmtIndex, i);
                    copyIn = new CopyInFromPath(Paths.get(fileName));
                    break;
                } catch (SQLException e) {
                    LOG.debug("COPY IN FROM clause parse filename error.", e);
                    String m = String.format("syntax error,FROM clause error,at near %s", fragment(sql, i));
                    throw PgExceptions.createSyntaxError(m);
                }
            }

        } // for
        if (copyIn == null) {
            throw PgExceptions.createSyntaxError("Not Found FROM clause in COPY.");
        }
        return copyIn;
    }


    @Override
    public final boolean isSingleStmt(String sql) throws SQLException {
        return (Boolean) doParse(sql, false);
    }

    private Object doParse(final String sql, final boolean createStmt) throws SQLException {
        if (!PgStrings.hasText(sql)) {
            throw PgExceptions.createSyntaxError("Statement couldn't be empty.");
        }
        final boolean isTrace = LOG.isTraceEnabled();
        final long startMillis = isTrace ? System.currentTimeMillis() : 0;
        final boolean confirmStringOff = confirmStringIsOff();
        final int sqlLength = sql.length();

        boolean inQuoteString = false, inCStyleEscapes = false, inUnicodeEscapes = false, inDoubleIdentifier = false;
        int stmtCount = 1;
        char ch;
        List<String> endpointList = new ArrayList<>();
        int lastParamEnd = 0;
        for (int i = 0; i < sqlLength; i++) {
            ch = sql.charAt(i);
            if (inQuoteString) {
                int index = sql.indexOf(QUOTE, i);
                if (index < 0) {
                    throw createQuoteNotCloseError(sql, i);
                }
                if ((confirmStringOff || inCStyleEscapes) && sql.charAt(index - 1) == BACK_SLASH) {
                    // C-Style Escapes
                    i = index;
                } else if (index + 1 < sqlLength && sql.charAt(index + 1) == QUOTE) {
                    // double quote Escapes
                    i = index + 1;
                } else {
                    i = index;
                    // LOG.debug("QUOTE end c-style[{}] unicode[{}]  ,current char[{}] at near {}",inCStyleEscapes,inUnicodeEscapes,ch,fragment(sql,i));
                    inQuoteString = false; // string constant end.
                    if (inUnicodeEscapes) {
                        inUnicodeEscapes = false;
                    } else if (inCStyleEscapes) {
                        inCStyleEscapes = false;
                    }
                }
                continue;
            } else if (inDoubleIdentifier) {
                int index = sql.indexOf(DOUBLE_QUOTE, i);
                if (index < 0) {
                    throw createDoubleQuotedIdentifierError(sql, i);
                }
                inDoubleIdentifier = false;
                if (inUnicodeEscapes) {
                    inUnicodeEscapes = false;
                }
                // LOG.debug("DOUBLE_QUOTE end ,current char[{}] sql fragment :{}",ch, fragment(sql,index));
                i = index;
                continue;
            }

            if (ch == QUOTE) {
                inQuoteString = true;
                //LOG.debug("QUOTE start ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
            } else if ((ch == 'E' || ch == 'e') && i + 1 < sqlLength && sql.charAt(i + 1) == QUOTE) {
                inQuoteString = inCStyleEscapes = true;
                //LOG.debug("QUOTE c-style start ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
                i++;
            } else if ((ch == 'U' || ch == 'u')
                    && i + 1 < sqlLength && sql.charAt(i + 1) == '&'
                    && i + 2 < sqlLength && sql.charAt(i + 2) == QUOTE) {
                inQuoteString = inUnicodeEscapes = true;
                // LOG.debug("QUOTE unicode start ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
                i += 2;
            } else if (ch == DOUBLE_QUOTE) {
                //LOG.debug("DOUBLE_QUOTE current char[{}] sql fragment :{}",ch, fragment(sql,i));
                inDoubleIdentifier = true;
            } else if ((ch == 'U' || ch == 'u')
                    && i + 1 < sqlLength && sql.charAt(i + 1) == '&'
                    && i + 2 < sqlLength && sql.charAt(i + 2) == DOUBLE_QUOTE) {
                // LOG.debug("DOUBLE_QUOTE with unicode ,current char[{}] sql fragment :{}",ch, fragment(sql,i));
                inDoubleIdentifier = inUnicodeEscapes = true;
                i += 2;
            } else if (ch == '$') {
                // Dollar-Quoted String Constants
                int index = sql.indexOf('$', i + 1);
                if (index < 0) {
                    throw PgExceptions.createSyntaxError("syntax error at or near \"$\"");
                }
                final String dollarTag = sql.substring(i, index + 1);
                index = sql.indexOf(dollarTag, index + 1);
                if (index < 0) {
                    String msg = String.format(
                            "syntax error,Dollar-Quoted String Constants not close, at or near \"%s\"", dollarTag);
                    throw PgExceptions.createSyntaxError(msg);
                }
                i = index + dollarTag.length() - 1;
            } else if (sql.startsWith(BLOCK_COMMENT_START_MARKER, i)) {
                i = skipBlockComment(sql, i);
            } else if (sql.startsWith(DOUBLE_DASH_COMMENT_MARKER, i)) {
                int index = sql.indexOf('\n', i + DOUBLE_DASH_COMMENT_MARKER.length());
                i = index > 0 ? index : sqlLength;
            } else if (ch == '?') {
                if (createStmt) {
                    endpointList.add(sql.substring(lastParamEnd, i));
                    lastParamEnd = i + 1;
                }
            } else if (ch == ';') {
                if (createStmt) {
                    String m = String.format(
                            "Detect multiple statements,multiple statements can't be bind,please check [%s].", sql);
                    throw PgExceptions.createSyntaxError(m);
                } else {
                    stmtCount++;
                }
            }

        }

        if (inQuoteString) {
            throw PgExceptions.createSyntaxError("syntax error,last string constants not close.");
        }
        if (inDoubleIdentifier) {
            throw PgExceptions.createSyntaxError("syntax error,last double quoted identifier not close.");
        }
        final Object parseResult;
        if (createStmt) {
            if (lastParamEnd < sqlLength) {
                endpointList.add(sql.substring(lastParamEnd));
            } else {
                endpointList.add("");
            }
            parseResult = PgStatementImpl.create(sql, endpointList);
        } else {
            parseResult = stmtCount == 1;
        }

        if (isTrace) {
            LOG.trace("SQL[{}] \nparse cost {} ms.", sql, System.currentTimeMillis() - startMillis);
        }
        return parseResult;
    }

    private boolean confirmStringIsOff() {
        String status = this.paramFunction.apply(ServerParameter.standard_conforming_strings);
        Objects.requireNonNull(status, "standard_conforming_strings value");
        return !ServerParameter.isOn(status);
    }

    /**
     * @return {@link #BLOCK_COMMENT_END_MARKER}'s last char index.
     */
    private static int skipBlockComment(final String sql, final int firstStartMarkerIndex)
            throws SQLException {

        final int length = sql.length(), markerLength = BLOCK_COMMENT_START_MARKER.length();
        final Stack<String> stack = new FastStack<>();
        final String errorMsg = "Block comment marker quote(/*) not close.";

        stack.push(BLOCK_COMMENT_START_MARKER);
        for (int i = firstStartMarkerIndex + markerLength, startMarkerIndex, endMarkerIndex; i < length; ) {
            endMarkerIndex = sql.indexOf(BLOCK_COMMENT_END_MARKER, i);
            if (endMarkerIndex < 0) {
                throw PgExceptions.createSyntaxError(errorMsg);
            }
            startMarkerIndex = sql.indexOf(BLOCK_COMMENT_START_MARKER, i);
            if (startMarkerIndex > 0 && startMarkerIndex < endMarkerIndex) {
                // nest, push start marker
                stack.push(BLOCK_COMMENT_START_MARKER);
            }
            stack.pop();
            if (stack.isEmpty()) {
                return endMarkerIndex + markerLength - 1;
            }
            // nest ,continue search
            i = endMarkerIndex + markerLength;
        }
        throw PgExceptions.createSyntaxError(errorMsg);
    }


    private static String fragment(String sql, int index) {
        return sql.substring(Math.max(index - 10, 0), Math.min(index + 10, sql.length()));
    }


    /**
     * @see #parseCopyIn(Stmt, int)
     */
    private String parseStringConstant(final Stmt stmt, final char[] charArray, final int stmtIndex, int i)
            throws SQLException {

        for (; i < charArray.length; i++) {
            if (!Character.isWhitespace(charArray[i])) {
                break;
            }
        }

        final String sql = stmt.getSql();
        final String stringConstant;

        final char ch = charArray[i];
        if (ch == '?') {
            stringConstant = extractStringConstantFromParamValue(stmt, stmtIndex, i);
        } else if (ch == QUOTE) {
            // string constant
            final boolean confirmStringOff = confirmStringIsOff();
            stringConstant = parseQuoteStringConstant(sql, charArray, i, confirmStringOff);
        } else if ((ch == 'e' || ch == 'E')
                && charArray[i + 1] == QUOTE) {
            // string constant with c-style escapes.
            stringConstant = parseQuoteStringConstant(sql, charArray, i + 1, false);
        } else if ((ch == 'u' || ch == 'U')
                && charArray[i + 1] == '&'
                && charArray[i + 2] == QUOTE) {
            // string constant with unicode escapes.
            stringConstant = parseQuoteStringConstant(sql, charArray, i + 2, true);
        } else if (ch == DOLLAR) {
            // dollar-quoted string constant
            stringConstant = parseDollarQuotedStringConstant(sql, i);
        } else {
            String m = String.format("Not found string constant at or near \"%s\" .", fragment(sql, i));
            throw PgExceptions.createSyntaxError(m);
        }

        return stringConstant;
    }

    /**
     * @see #parseCopyIn(Stmt, int)
     */
    private static String parseQuoteStringConstant(final String sql, final char[] charArray, final int i
            , final boolean recognizesBackslash)
            throws SQLException {
        final int lastIndex = charArray.length - 1;

        for (int j = i + 1; j < charArray.length; j++) {
            if (charArray[j] != QUOTE) {
                continue;
            }
            if (recognizesBackslash && charArray[j - 1] == BACK_SLASH) {
                // C-Style Escapes
                continue;
            } else if (j < lastIndex && charArray[j + 1] == QUOTE) {
                //  double quote Escapes
                j++;
                continue;
            }
            return sql.substring(i + 1, j);
        }
        throw createQuoteNotCloseError(sql, i);
    }

    /**
     * @see #parseCopyIn(Stmt, int)
     */
    private static String parseDollarQuotedStringConstant(final String sql, final int i) throws SQLException {
        final int tagIndex = sql.indexOf('$', i + 1);
        if (tagIndex < 0) {
            throw createDollarQuotedStringConstantError(sql, i);
        }
        final int constantStartIndex = tagIndex + 1;
        final String tag = sql.substring(i, constantStartIndex);
        final int constantEndIndex = sql.indexOf(tag, constantStartIndex);
        if (constantEndIndex < 0) {
            throw createDollarQuotedNotCloseError(sql, i);
        }
        return sql.substring(constantStartIndex, constantEndIndex);
    }


    /**
     * @see #parseCopyIn(Stmt, int)
     */
    private static String extractStringConstantFromParamValue(Stmt stmt, final int stmtIndex, final int i)
            throws SQLException {

        final List<? extends ParamValue> paramList;
        if (stmt instanceof BatchBindStmt) {
            List<List<BindValue>> groupList = ((BatchBindStmt) stmt).getGroupList();
            if (stmtIndex < groupList.size()) {
                paramList = groupList.get(stmtIndex);
            } else {
                // here bug.
                String m = String.format(
                        "Not found param group for stmtIndex[%s] for placeholder at near %s"
                        , stmtIndex, fragment(stmt.getSql(), i));
                throw PgExceptions.createSyntaxError(m);
            }
        } else if (stmt instanceof ParamStmt) {
            paramList = ((ParamStmt) stmt).getParamGroup();
        } else {
            String m = String.format(
                    "syntax error,FROM clause error,not found bind value,at near %s"
                    , fragment(stmt.getSql(), i));
            throw PgExceptions.createSyntaxError(m);
        }

        if (paramList.isEmpty()) {
            String m = String.format(
                    "bind error,FROM clause error,not found bind string constant,at near %s"
                    , fragment(stmt.getSql(), i));
            throw PgExceptions.createSyntaxError(m);
        }
        ParamValue paramValue = paramList.get(0);
        final Object value = paramValue.getValue();
        if (!(value instanceof String)) {
            String m = String.format(
                    "bind error,FROM clause error,not found bind value isn't java.lang.String instance,at near %s"
                    , fragment(stmt.getSql(), i));
            throw PgExceptions.createSyntaxError(m);
        }
        return (String) value;
    }

    private static SQLException createQuoteNotCloseError(String sql, int index) {
        String m = String.format("Syntax error,string constants not close at near %s .", fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static SQLException createDollarQuotedStringConstantError(String sql, int index) {
        String m = String.format("Dollar quoted string constant syntax error at or near \"%s\" .", fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static SQLException createDollarQuotedNotCloseError(String sql, int index) {
        String m = String.format("Syntax error,dollar quoted string constant not close,at or near \"%s\" ."
                , fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }

    private static SQLException createDoubleQuotedIdentifierError(String sql, int index) {
        String m = String.format(
                "syntax error,double quoted identifier not close,at near %s", fragment(sql, index));
        return PgExceptions.createSyntaxError(m);
    }


    private static final class CopyInFromPath implements CopyIn {

        private final Path path;

        private CopyInFromPath(Path path) {
            this.path = path;
        }


        @Override
        public final Mode getMode() {
            return Mode.FILE;
        }

        @Override
        public final Path getPath() {
            return this.path;
        }

        @Override
        public final String getCommand() {
            throw new IllegalStateException(String.format("Mode isn't %s .", Mode.PROGRAM));
        }

        @Override
        public final Function<String, Publisher<byte[]>> getFunction() {
            throw new IllegalStateException(String.format("Mode one of [%s,%s] .", Mode.PROGRAM, Mode.STDIN));
        }

    }

    private static final class CopyInFromProgram implements CopyIn {

        private final String command;

        private final Function<String, Publisher<byte[]>> function;

        private CopyInFromProgram(String command, Function<String, Publisher<byte[]>> function) {
            this.command = command;
            this.function = function;
        }

        @Override
        public final Mode getMode() {
            return Mode.PROGRAM;
        }

        @Override
        public final String getCommand() {
            return this.command;
        }

        @Override
        public final Path getPath() {
            throw new IllegalStateException(String.format("Mode isn't %s .", Mode.FILE));
        }

        @Override
        public final Function<String, Publisher<byte[]>> getFunction() {
            return this.function;
        }

    }

    private static final class CopyInFromStdin implements CopyIn {

        private final Function<String, Publisher<byte[]>> function;

        private CopyInFromStdin(Function<String, Publisher<byte[]>> function) {
            this.function = function;
        }

        @Override
        public final Mode getMode() {
            return Mode.STDIN;
        }

        @Override
        public final Path getPath() {
            throw new IllegalStateException(String.format("Mode isn't %s .", Mode.FILE));
        }

        @Override
        public final String getCommand() {
            throw new IllegalStateException(String.format("Mode isn't %s .", Mode.PROGRAM));
        }

        @Override
        public final Function<String, Publisher<byte[]>> getFunction() {
            return this.function;
        }

    }


}
