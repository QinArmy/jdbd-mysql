package io.jdbd.mysql.util;

import io.jdbd.mysql.protocol.Constants;
import io.jdbd.util.JdbdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 * This class is the test class of {@link MySQLStrings}
 * </p>
 */
public class MySQLStringsTests {

    private static final Logger LOG = LoggerFactory.getLogger(MySQLStringsTests.class);

    /**
     * @see MySQLStrings#appendLiteral(String, boolean, StringBuilder)
     */
    @Test
    public void appendLiteral() {
        String text;
        final StringBuilder builder = new StringBuilder();

        MySQLStrings.appendLiteral(null, true, builder);
        Assert.assertEquals(builder.toString(), Constants.NULL);


        builder.setLength(0);
        MySQLStrings.appendLiteral(null, false, builder);
        Assert.assertEquals(builder.toString(), Constants.NULL);


        builder.setLength(0);
        MySQLStrings.appendLiteral("army", true, builder);
        Assert.assertEquals("'army'", builder.toString());

        builder.setLength(0);
        MySQLStrings.appendLiteral("army", false, builder);
        Assert.assertEquals(builder.toString(), "'army'");

        builder.setLength(0);
        text = "中国QinArmy's army, \0 \032 \b \n \r \t \" \\ ";
        MySQLStrings.appendLiteral(text, true, builder);
        Assert.assertEquals(builder.toString(), "'中国QinArmy''s army, \0 \\Z \b \n \r \t \" \\\\ '");

        builder.setLength(0);
        MySQLStrings.appendLiteral(text, false, builder);
        Assert.assertEquals(builder.toString(), "_utf8mb4 0x" + JdbdUtils.hexEscapesText(true, text.getBytes(StandardCharsets.UTF_8)));

        builder.setLength(0);
        MySQLStrings.appendLiteral("中国QinArmy's army \0 \b \n \r \t \" ", false, builder);
        Assert.assertEquals(builder.toString(), "'中国QinArmy''s army \0 \b \n \r \t \" '");

        builder.setLength(0);
        text = "中国QinArmy's army, \0 \032 \b \n \r \t \" \\ \\Z ";
        MySQLStrings.appendLiteral(text, true, builder);
        Assert.assertEquals(builder.toString(), "'中国QinArmy''s army, \0 \\Z \b \n \r \t \" \\\\ \\\\Z '");

        builder.setLength(0);
        text = "中国QinArmy's army, \0 \b \n \r \t \" ";
        MySQLStrings.appendLiteral(text, false, builder);
        Assert.assertEquals(builder.toString(), "'中国QinArmy''s army, \0 \b \n \r \t \" '");


    }

    /**
     * @see MySQLStrings#appendMySqlIdentifier(String, StringBuilder)
     */
    @Test
    public void appendMySqlIdentifier() {
        String text;
        final StringBuilder builder = new StringBuilder();


        text = "$中国QinArmy_123";
        MySQLStrings.appendMySqlIdentifier(text, builder);
        Assert.assertEquals(builder.toString(), text);

        builder.setLength(0);
        text = "4343army";
        MySQLStrings.appendMySqlIdentifier(text, builder);
        Assert.assertEquals(builder.toString(), "`4343army`");


        builder.setLength(0);
        text = "中国QinArmy's army";
        MySQLStrings.appendMySqlIdentifier(text, builder);
        Assert.assertEquals(builder.toString(), "`中国QinArmy's army`");

    }

    /**
     * @see MySQLStrings#appendMySqlIdentifier(String, StringBuilder)
     */
    @Test
    public void appendMySqlIdentifierError() {
        Assert.assertNotNull(MySQLStrings.appendMySqlIdentifier("There is ` ", new StringBuilder()));
    }


}
