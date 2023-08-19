package io.jdbd.mysql.protocol.client;


import io.jdbd.JdbdException;
import io.jdbd.mysql.protocol.MySQLServerVersion;
import io.jdbd.mysql.util.MySQLCollections;
import reactor.util.annotation.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

public abstract class Charsets {

    protected Charsets() {
        throw new UnsupportedOperationException();
    }


    private static final String armscii8 = "armscii8";
    private static final String ascii = "ascii";
    private static final String big5 = "big5";
    private static final String binary = "binary";

    private static final String cp1250 = "cp1250";
    private static final String cp1251 = "cp1251";
    private static final String cp1256 = "cp1256";

    private static final String cp1257 = "cp1257";
    private static final String cp850 = "cp850";
    private static final String cp852 = "cp852";

    private static final String cp866 = "cp866";
    private static final String cp932 = "cp932";
    private static final String dec8 = "dec8";
    private static final String eucjpms = "eucjpms";

    private static final String euckr = "euckr";
    private static final String gb18030 = "gb18030";
    private static final String gb2312 = "gb2312";
    private static final String gbk = "gbk";

    private static final String geostd8 = "geostd8";
    private static final String greek = "greek";
    private static final String hebrew = "hebrew";
    private static final String hp8 = "hp8";

    private static final String keybcs2 = "keybcs2";
    private static final String koi8r = "koi8r";
    private static final String koi8u = "koi8u";
    private static final String latin1 = "latin1";

    private static final String latin2 = "latin2";
    private static final String latin5 = "latin5";
    private static final String latin7 = "latin7";
    private static final String macce = "macce";

    private static final String macroman = "macroman";
    private static final String sjis = "sjis";
    private static final String swe7 = "swe7";
    private static final String tis620 = "tis620";

    private static final String ucs2 = "ucs2";
    private static final String ujis = "ujis";
    private static final String utf16 = "utf16";
    private static final String utf16le = "utf16le";

    private static final String utf32 = "utf32";
    private static final String utf8 = "utf8";
    public static final String utf8mb4 = "utf8mb4";

    public static final String NOT_USED = latin1; // punting for not-used character sets
    public static final String COLLATION_NOT_DEFINED = "none";

    public static final int MYSQL_COLLATION_INDEX_utf8 = 33;
    public static final int MYSQL_COLLATION_INDEX_binary = 63;
    public static final int MYSQL_COLLATION_INDEX_utf8mb4 = 255;

    private static final int NUMBER_OF_ENCODINGS_CONFIGURED;

    /**
     * a unmodifiable map
     */
    public static final Map<String, MyCharset> NAME_TO_CHARSET;

    /**
     * a unmodifiable map
     */
    public static final Map<String, List<MyCharset>> JAVA_ENCODING_UC_TO_MYSQL_CHARSET;

    /**
     * a unmodifiable map
     */
    public static final Set<String> MULTIBYTE_ENCODINGS;

    /**
     * a unmodifiable map
     */
    public static final Map<String, Integer> CHARSET_NAME_TO_COLLATION_INDEX;

    /**
     * a unmodifiable map
     */
    public static final Set<Integer> UTF8MB4_INDEXES;

    /**
     * a unmodifiable map
     */
    public static final Map<Integer, Collation> INDEX_TO_COLLATION;

    /**
     * a unmodifiable map
     */
    public static final Map<String, Collation> NAME_TO_COLLATION;

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html#charset-connection-impermissible-client-charset">Impermissible Client Character Sets</a>
     */
    public static final Collection<String> UNSUPPORTED_CHARSET_CLIENTS = createUnsupportedCharsetClients();


    static {
        // 1. below initialize fore: NUMBER_OF_ENCODINGS_CONFIGURED,CHARSET_NAME_TO_CHARSET ,JAVA_ENCODING_UC_TO_MYSQL_CHARSET,MULTIBYTE_ENCODINGS
        final List<MyCharset> myCharsetList = createMySQLCharsetList();

        Map<String, MyCharset> charsetNameToMysqlCharsetMap = MySQLCollections.hashMap();
        Map<String, List<MyCharset>> javaUcToMysqlCharsetMap = MySQLCollections.hashMap();

        Set<String> tempMultibyteEncodings = new HashSet<>(); // Character sets that we can't convert ourselves.

        int numberOfEncodingsConfigured = 0;
        for (MyCharset myCharset : myCharsetList) {
            charsetNameToMysqlCharsetMap.put(myCharset.name, myCharset);
            numberOfEncodingsConfigured += myCharset.javaEncodingsUcList.size();

            for (String encUC : myCharset.javaEncodingsUcList) {
                List<MyCharset> charsetList = javaUcToMysqlCharsetMap.computeIfAbsent(encUC, k -> new ArrayList<>());
                charsetList.add(myCharset);
                if (myCharset.mblen > 1) {
                    tempMultibyteEncodings.add(encUC);
                }
            }
        }
        NUMBER_OF_ENCODINGS_CONFIGURED = numberOfEncodingsConfigured;
        NAME_TO_CHARSET = Collections.unmodifiableMap(charsetNameToMysqlCharsetMap);
        JAVA_ENCODING_UC_TO_MYSQL_CHARSET = Collections.unmodifiableMap(javaUcToMysqlCharsetMap);
        MULTIBYTE_ENCODINGS = Collections.unmodifiableSet(tempMultibyteEncodings);

        // 2. below initialize four : CHARSET_NAME_TO_COLLATION_INDEX,UTF8MB4_INDEXES,INDEX_TO_COLLATION,NAME_TO_COLLATION
        final int maxSize = 2048;

        //final Collation notUsedCollation = new Collation(0, COLLATION_NOT_DEFINED, 0, NOT_USED);
        final Map<Integer, Collation> indexToCollationMap = MySQLCollections.hashMap((int) (maxSize / 0.75f));

        Map<String, Integer> charsetNameToCollationIndexMap = new TreeMap<>();
        Map<String, Integer> charsetNameToCollationPriorityMap = new TreeMap<>();
        Set<Integer> tempUTF8MB4Indexes = MySQLCollections.hashSet();

        Map<String, Collation> nameToCollation = MySQLCollections.hashMap();

        for (Map.Entry<Integer, Collation> e : createCollationMap().entrySet()) {
            Integer i = e.getKey();
            Collation collation = e.getValue();

            indexToCollationMap.put(i, collation);
            nameToCollation.put(collation.name, collation);

            String charsetName = collation.myCharset.name;
            if (!charsetNameToCollationIndexMap.containsKey(charsetName)
                    || charsetNameToCollationPriorityMap.get(charsetName) < collation.priority) {
                charsetNameToCollationIndexMap.put(charsetName, i);
                charsetNameToCollationPriorityMap.put(charsetName, collation.priority);
            }
            // Filling indexes of utf8mb4 collations
            if (charsetName.equals(utf8mb4)) {
                tempUTF8MB4Indexes.add(i);
            }
        }

        CHARSET_NAME_TO_COLLATION_INDEX = Collections.unmodifiableMap(charsetNameToCollationIndexMap);
        UTF8MB4_INDEXES = Collections.unmodifiableSet(tempUTF8MB4Indexes);
        INDEX_TO_COLLATION = Collections.unmodifiableMap(indexToCollationMap);
        NAME_TO_COLLATION = Collections.unmodifiableMap(nameToCollation);

    }


    public static int getNumberOfCharsetsConfigured() {
        return NUMBER_OF_ENCODINGS_CONFIGURED;
    }

    @Nullable
    public static String getJavaCharsetByIndex(int collationIndex) {
        Collation collation = INDEX_TO_COLLATION.get(collationIndex);
        if (collation == null) {
            return null;
        }
        return collation.myCharset.javaEncodingsUcList.get(0);
    }

    public static int getMblen(int collationIndex) {
        Collation collation = INDEX_TO_COLLATION.get(collationIndex);
        if (collation == null || COLLATION_NOT_DEFINED.equals(collation.name)) {
            String m = String.format("Not found Collation for collationIndex[%s]", collationIndex);
            throw new JdbdException(m);
        }
        return collation.myCharset.mblen;
    }

    @Nullable
    public static MyCharset getMysqlCharsetForJavaEncoding(String javaEncoding, @Nullable MySQLServerVersion version) {
        List<MyCharset> mysqlCharsets;
        mysqlCharsets = Charsets.JAVA_ENCODING_UC_TO_MYSQL_CHARSET.get(javaEncoding.toUpperCase());

        if (mysqlCharsets == null) {
            return null;
        }
        MyCharset currentChoice = null;
        for (MyCharset charset : mysqlCharsets) {
            if (version == null) {
                // Take the first one we get
                return charset;
            }

            if (currentChoice == null
                    || currentChoice.minimumVersion.compareTo(charset.minimumVersion) < 0
                    || (currentChoice.priority < charset.priority
                    && currentChoice.minimumVersion.compareTo(charset.minimumVersion) == 0)) {
                if (charset.isOkayForVersion(version)) {
                    currentChoice = charset;
                }
            }
        }
        return currentChoice;
    }

    /**
     * exclude not use {@link Collation}
     */
    @Nullable
    public static Charset getJavaCharsetByCollationIndex(int collationIndex) {
        Collation collation = INDEX_TO_COLLATION.get(collationIndex);
        if (collation == null || COLLATION_NOT_DEFINED.equals(collation.name)) {
            return null;
        }
        return Charset.forName(collation.myCharset.javaEncodingsUcList.get(0));
    }

    public static Charset getJavaCharsetByCollationIndex(int collationIndex,
                                                         Map<Integer, Charsets.CustomCollation> customCollationMap)
            throws JdbdException {
        Collation collation = INDEX_TO_COLLATION.get(collationIndex);
        Charset charset = getJavaCharsetByCollationIndex(collationIndex);
        if (charset == null) {
            Charsets.CustomCollation customCollation = customCollationMap.get(collationIndex);
            if (customCollation == null) {
                String m = String.format("Not found collation for index[%s]", collation);
                throw new JdbdException(m);
            }
            charset = getJavaCharsetByMySQLCharsetName(customCollation.charsetName);
            if (charset == null) {
                String m = String.format("Not found java charset for name[%s]", customCollation.charsetName);
                throw new JdbdException(m);
            }
        } else {
            charset = Charset.forName(collation.myCharset.javaEncodingsUcList.get(0));
        }
        return charset;
    }

    @Nullable
    public static Charset getJavaCharsetByMySQLCharsetName(String mysqlCharsetName) {
        MyCharset myCharset = NAME_TO_CHARSET.get(mysqlCharsetName);
        if (myCharset == null) {
            return null;
        }
        return Charset.forName(myCharset.javaEncodingsUcList.get(0));
    }


    public static int getCollationIndexForJavaEncoding(String javaEncoding, MySQLServerVersion version) {
        MyCharset myCharset = getMysqlCharsetForJavaEncoding(javaEncoding, version);
        if (myCharset != null) {
            Integer ci = CHARSET_NAME_TO_COLLATION_INDEX.get(myCharset.name);
            if (ci != null) {
                return ci;
            }
        }
        return 0;
    }

    @Nullable
    public static String getCollationNameByIndex(int collationIndex) {
        Collation collation = INDEX_TO_COLLATION.get(collationIndex);
        return collation == null ? null : collation.name;
    }

    @Nullable
    public static Collation getCollationByName(final String collationName) {
        return Charsets.NAME_TO_COLLATION.get(collationName.toLowerCase());
    }

    public static boolean isUnsupportedCharsetClient(String javaCharset) {
        return UNSUPPORTED_CHARSET_CLIENTS.contains(javaCharset.toUpperCase())
                || UNSUPPORTED_CHARSET_CLIENTS.contains(javaCharset.toLowerCase());
    }

    public static boolean isSupportCharsetClient(final Charset charset) {
        final String keyText = "\0'\032;\\";
        final byte[] keyBytes;
        keyBytes = keyText.getBytes(StandardCharsets.US_ASCII);
        boolean support;
        try {
            final byte[] bytes;
            bytes = keyText.getBytes(charset);
            support = Arrays.equals(bytes, keyBytes);
        } catch (Throwable e) {
            support = false;
        }
        return support;
    }


    /**
     * @return a unmodifiable list
     */
    private static List<MyCharset> createMySQLCharsetList() {
        final List<MyCharset> list = MySQLCollections.arrayList(41);
        // complete list of mysql character sets and their corresponding java encoding names
        list.add(new MyCharset(ascii, 1, 0, "US-ASCII", "ASCII"));
        list.add(new MyCharset(big5, 2, 0, "Big5"));
        list.add(new MyCharset(gbk, 2, 0, "GBK"));
        list.add(new MyCharset(sjis, 2, 0, "SHIFT_JIS", "Cp943", "WINDOWS-31J"));    // SJIS is alias for SHIFT_JIS, Cp943 is rather a cp932 but we map it to sjis for years

        list.add(new MyCharset(cp932, 2, 1, "WINDOWS-31J"));        // MS932 is alias for WINDOWS-31J
        list.add(new MyCharset(gb2312, 2, 0, "GB2312"));
        list.add(new MyCharset(ujis, 3, 0, "EUC_JP"));
        list.add(new MyCharset(eucjpms, 3, 0, MySQLServerVersion.getInstance(5, 0, 3), "EUC_JP_Solaris"));    // "EUC_JP_Solaris = 	>5.0.3 eucjpms,"

        list.add(new MyCharset(gb18030, 4, 0, MySQLServerVersion.getInstance(5, 7, 4), "GB18030"));
        list.add(new MyCharset(euckr, 2, 0, "EUC-KR"));
        list.add(new MyCharset(latin1, 1, 1, "Cp1252", "ISO8859_1"));
        list.add(new MyCharset(swe7, 1, 0, "Cp1252"));            // new mapping, Cp1252 ?

        list.add(new MyCharset(hp8, 1, 0, "Cp1252"));            // new mapping, Cp1252 ?
        list.add(new MyCharset(dec8, 1, 0, "Cp1252"));            // new mapping, Cp1252 ?
        list.add(new MyCharset(armscii8, 1, 0, "Cp1252"));            // new mapping, Cp1252 ?
        list.add(new MyCharset(geostd8, 1, 0, "Cp1252"));            // new mapping, Cp1252 ?

        list.add(new MyCharset(latin2, 1, 0, "ISO8859_2"));        // latin2 is an alias
        list.add(new MyCharset(greek, 1, 0, "ISO8859_7", "greek"));
        list.add(new MyCharset(latin7, 1, 0, "ISO-8859-13"));    // was ISO8859_7, that's incorrect; also + "LATIN7 =		latin7," is wrong java encoding name
        list.add(new MyCharset(hebrew, 1, 0, "ISO8859_8"));        // hebrew is an alias

        list.add(new MyCharset(latin5, 1, 0, "ISO8859_9"));        // LATIN5 is an alias
        list.add(new MyCharset(cp850, 1, 0, "Cp850", "Cp437"));
        list.add(new MyCharset(cp852, 1, 0, "Cp852"));
        list.add(new MyCharset(keybcs2, 1, 0, "Cp852"));    // new, Kamenicky encoding usually known as Cp895 but there is no official cp895 specification; close to Cp852, see http://ftp.muni.cz/pub/localization/charsets/cs-encodings-faq

        list.add(new MyCharset(cp866, 1, 0, "Cp866"));
        list.add(new MyCharset(koi8r, 1, 1, "KOI8_R"));
        list.add(new MyCharset(koi8u, 1, 0, "KOI8_R"));
        list.add(new MyCharset(tis620, 1, 0, "TIS620"));

        list.add(new MyCharset(cp1250, 1, 0, "Cp1250"));
        list.add(new MyCharset(cp1251, 1, 1, "Cp1251"));
        list.add(new MyCharset(cp1256, 1, 0, "Cp1256"));
        list.add(new MyCharset(cp1257, 1, 0, "Cp1257"));

        list.add(new MyCharset(macroman, 1, 0, "MacRoman"));
        list.add(new MyCharset(macce, 1, 0, "MacCentralEurope"));
        list.add(new MyCharset(utf8, 3, 1, "UTF-8"));
        list.add(new MyCharset(utf8mb4, 4, 0, "UTF-8"));            // "UTF-8 =				*> 5.5.2 utf8mb4,"

        list.add(new MyCharset(ucs2, 2, 0, "UnicodeBig"));
        list.add(new MyCharset(binary, 1, 1, "ISO8859_1"));    // US-ASCII ?
        list.add(new MyCharset(utf16, 4, 0, "UTF-16"));
        list.add(new MyCharset(utf16le, 4, 0, "UTF-16LE"));

        list.add(new MyCharset(utf32, 4, 0, "UTF-32"));

        return Collections.unmodifiableList(list);
    }

    private static List<Collation> createCollationList() {
        final List<Collation> list = MySQLCollections.arrayList(430);

        // complete list of mysql collations and their corresponding character sets each element of collation[1]..collation[MAP_SIZE-1] must not be null
        list.add(new Collation(1, "big5_chinese_ci", 1, big5));
        list.add(new Collation(2, "latin2_czech_cs", 0, latin2));
        list.add(new Collation(3, "dec8_swedish_ci", 0, dec8));
        list.add(new Collation(4, "cp850_general_ci", 1, cp850));
        list.add(new Collation(5, "latin1_german1_ci", 0, latin1));
        list.add(new Collation(6, "hp8_english_ci", 0, hp8));
        list.add(new Collation(7, "koi8r_general_ci", 0, koi8r));
        list.add(new Collation(8, "latin1_swedish_ci", 1, latin1));
        list.add(new Collation(9, "latin2_general_ci", 1, latin2));
        list.add(new Collation(10, "swe7_swedish_ci", 0, swe7));
        list.add(new Collation(11, "ascii_general_ci", 0, ascii));
        list.add(new Collation(12, "ujis_japanese_ci", 0, ujis));
        list.add(new Collation(13, "sjis_japanese_ci", 0, sjis));
        list.add(new Collation(14, "cp1251_bulgarian_ci", 0, cp1251));
        list.add(new Collation(15, "latin1_danish_ci", 0, latin1));
        list.add(new Collation(16, "hebrew_general_ci", 0, hebrew));

        list.add(new Collation(18, "tis620_thai_ci", 0, tis620));
        list.add(new Collation(19, "euckr_korean_ci", 0, euckr));
        list.add(new Collation(20, "latin7_estonian_cs", 0, latin7));
        list.add(new Collation(21, "latin2_hungarian_ci", 0, latin2));
        list.add(new Collation(22, "koi8u_general_ci", 0, koi8u));
        list.add(new Collation(23, "cp1251_ukrainian_ci", 0, cp1251));
        list.add(new Collation(24, "gb2312_chinese_ci", 0, gb2312));
        list.add(new Collation(25, "greek_general_ci", 0, greek));
        list.add(new Collation(26, "cp1250_general_ci", 1, cp1250));
        list.add(new Collation(27, "latin2_croatian_ci", 0, latin2));
        list.add(new Collation(28, "gbk_chinese_ci", 1, gbk));
        list.add(new Collation(29, "cp1257_lithuanian_ci", 0, cp1257));
        list.add(new Collation(30, "latin5_turkish_ci", 1, latin5));
        list.add(new Collation(31, "latin1_german2_ci", 0, latin1));
        list.add(new Collation(32, "armscii8_general_ci", 0, armscii8));
        list.add(new Collation(MYSQL_COLLATION_INDEX_utf8, "utf8_general_ci", 1, utf8));
        list.add(new Collation(34, "cp1250_czech_cs", 0, cp1250));
        list.add(new Collation(35, "ucs2_general_ci", 1, ucs2));
        list.add(new Collation(36, "cp866_general_ci", 1, cp866));
        list.add(new Collation(37, "keybcs2_general_ci", 1, keybcs2));
        list.add(new Collation(38, "macce_general_ci", 1, macce));
        list.add(new Collation(39, "macroman_general_ci", 1, macroman));
        list.add(new Collation(40, "cp852_general_ci", 1, cp852));
        list.add(new Collation(41, "latin7_general_ci", 1, latin7));
        list.add(new Collation(42, "latin7_general_cs", 0, latin7));
        list.add(new Collation(43, "macce_bin", 0, macce));
        list.add(new Collation(44, "cp1250_croatian_ci", 0, cp1250));
        list.add(new Collation(45, "utf8mb4_general_ci", 0, utf8mb4));
        list.add(new Collation(46, "utf8mb4_bin", 0, utf8mb4));
        list.add(new Collation(47, "latin1_bin", 0, latin1));
        list.add(new Collation(48, "latin1_general_ci", 0, latin1));
        list.add(new Collation(49, "latin1_general_cs", 0, latin1));
        list.add(new Collation(50, "cp1251_bin", 0, cp1251));
        list.add(new Collation(51, "cp1251_general_ci", 1, cp1251));
        list.add(new Collation(52, "cp1251_general_cs", 0, cp1251));
        list.add(new Collation(53, "macroman_bin", 0, macroman));
        list.add(new Collation(54, "utf16_general_ci", 1, utf16));
        list.add(new Collation(55, "utf16_bin", 0, utf16));
        list.add(new Collation(56, "utf16le_general_ci", 1, utf16le));
        list.add(new Collation(57, "cp1256_general_ci", 1, cp1256));
        list.add(new Collation(58, "cp1257_bin", 0, cp1257));
        list.add(new Collation(59, "cp1257_general_ci", 1, cp1257));
        list.add(new Collation(60, "utf32_general_ci", 1, utf32));
        list.add(new Collation(61, "utf32_bin", 0, utf32));
        list.add(new Collation(62, "utf16le_bin", 0, utf16le));
        list.add(new Collation(MYSQL_COLLATION_INDEX_binary, "binary", 1, binary));
        list.add(new Collation(64, "armscii8_bin", 0, armscii8));
        list.add(new Collation(65, "ascii_bin", 0, ascii));
        list.add(new Collation(66, "cp1250_bin", 0, cp1250));
        list.add(new Collation(67, "cp1256_bin", 0, cp1256));
        list.add(new Collation(68, "cp866_bin", 0, cp866));
        list.add(new Collation(69, "dec8_bin", 0, dec8));
        list.add(new Collation(70, "greek_bin", 0, greek));
        list.add(new Collation(71, "hebrew_bin", 0, hebrew));
        list.add(new Collation(72, "hp8_bin", 0, hp8));
        list.add(new Collation(73, "keybcs2_bin", 0, keybcs2));
        list.add(new Collation(74, "koi8r_bin", 0, koi8r));
        list.add(new Collation(75, "koi8u_bin", 0, koi8u));
        list.add(new Collation(76, "utf8_tolower_ci", 0, utf8));
        list.add(new Collation(77, "latin2_bin", 0, latin2));
        list.add(new Collation(78, "latin5_bin", 0, latin5));
        list.add(new Collation(79, "latin7_bin", 0, latin7));
        list.add(new Collation(80, "cp850_bin", 0, cp850));
        list.add(new Collation(81, "cp852_bin", 0, cp852));
        list.add(new Collation(82, "swe7_bin", 0, swe7));
        list.add(new Collation(83, "utf8_bin", 0, utf8));
        list.add(new Collation(84, "big5_bin", 0, big5));
        list.add(new Collation(85, "euckr_bin", 0, euckr));
        list.add(new Collation(86, "gb2312_bin", 0, gb2312));
        list.add(new Collation(87, "gbk_bin", 0, gbk));
        list.add(new Collation(88, "sjis_bin", 0, sjis));
        list.add(new Collation(89, "tis620_bin", 0, tis620));
        list.add(new Collation(90, "ucs2_bin", 0, ucs2));
        list.add(new Collation(91, "ujis_bin", 0, ujis));
        list.add(new Collation(92, "geostd8_general_ci", 0, geostd8));
        list.add(new Collation(93, "geostd8_bin", 0, geostd8));
        list.add(new Collation(94, "latin1_spanish_ci", 0, latin1));
        list.add(new Collation(95, "cp932_japanese_ci", 1, cp932));
        list.add(new Collation(96, "cp932_bin", 0, cp932));
        list.add(new Collation(97, "eucjpms_japanese_ci", 1, eucjpms));
        list.add(new Collation(98, "eucjpms_bin", 0, eucjpms));
        list.add(new Collation(99, "cp1250_polish_ci", 0, cp1250));

        list.add(new Collation(101, "utf16_unicode_ci", 0, utf16));
        list.add(new Collation(102, "utf16_icelandic_ci", 0, utf16));
        list.add(new Collation(103, "utf16_latvian_ci", 0, utf16));
        list.add(new Collation(104, "utf16_romanian_ci", 0, utf16));
        list.add(new Collation(105, "utf16_slovenian_ci", 0, utf16));
        list.add(new Collation(106, "utf16_polish_ci", 0, utf16));
        list.add(new Collation(107, "utf16_estonian_ci", 0, utf16));
        list.add(new Collation(108, "utf16_spanish_ci", 0, utf16));
        list.add(new Collation(109, "utf16_swedish_ci", 0, utf16));
        list.add(new Collation(110, "utf16_turkish_ci", 0, utf16));
        list.add(new Collation(111, "utf16_czech_ci", 0, utf16));
        list.add(new Collation(112, "utf16_danish_ci", 0, utf16));
        list.add(new Collation(113, "utf16_lithuanian_ci", 0, utf16));
        list.add(new Collation(114, "utf16_slovak_ci", 0, utf16));
        list.add(new Collation(115, "utf16_spanish2_ci", 0, utf16));
        list.add(new Collation(116, "utf16_roman_ci", 0, utf16));
        list.add(new Collation(117, "utf16_persian_ci", 0, utf16));
        list.add(new Collation(118, "utf16_esperanto_ci", 0, utf16));
        list.add(new Collation(119, "utf16_hungarian_ci", 0, utf16));
        list.add(new Collation(120, "utf16_sinhala_ci", 0, utf16));
        list.add(new Collation(121, "utf16_german2_ci", 0, utf16));
        list.add(new Collation(122, "utf16_croatian_ci", 0, utf16));
        list.add(new Collation(123, "utf16_unicode_520_ci", 0, utf16));
        list.add(new Collation(124, "utf16_vietnamese_ci", 0, utf16));

        list.add(new Collation(128, "ucs2_unicode_ci", 0, ucs2));
        list.add(new Collation(129, "ucs2_icelandic_ci", 0, ucs2));
        list.add(new Collation(130, "ucs2_latvian_ci", 0, ucs2));
        list.add(new Collation(131, "ucs2_romanian_ci", 0, ucs2));
        list.add(new Collation(132, "ucs2_slovenian_ci", 0, ucs2));
        list.add(new Collation(133, "ucs2_polish_ci", 0, ucs2));
        list.add(new Collation(134, "ucs2_estonian_ci", 0, ucs2));
        list.add(new Collation(135, "ucs2_spanish_ci", 0, ucs2));
        list.add(new Collation(136, "ucs2_swedish_ci", 0, ucs2));
        list.add(new Collation(137, "ucs2_turkish_ci", 0, ucs2));
        list.add(new Collation(138, "ucs2_czech_ci", 0, ucs2));
        list.add(new Collation(139, "ucs2_danish_ci", 0, ucs2));
        list.add(new Collation(140, "ucs2_lithuanian_ci", 0, ucs2));
        list.add(new Collation(141, "ucs2_slovak_ci", 0, ucs2));
        list.add(new Collation(142, "ucs2_spanish2_ci", 0, ucs2));
        list.add(new Collation(143, "ucs2_roman_ci", 0, ucs2));
        list.add(new Collation(144, "ucs2_persian_ci", 0, ucs2));
        list.add(new Collation(145, "ucs2_esperanto_ci", 0, ucs2));
        list.add(new Collation(146, "ucs2_hungarian_ci", 0, ucs2));
        list.add(new Collation(147, "ucs2_sinhala_ci", 0, ucs2));
        list.add(new Collation(148, "ucs2_german2_ci", 0, ucs2));
        list.add(new Collation(149, "ucs2_croatian_ci", 0, ucs2));
        list.add(new Collation(150, "ucs2_unicode_520_ci", 0, ucs2));
        list.add(new Collation(151, "ucs2_vietnamese_ci", 0, ucs2));

        list.add(new Collation(159, "ucs2_general_mysql500_ci", 0, ucs2));
        list.add(new Collation(160, "utf32_unicode_ci", 0, utf32));
        list.add(new Collation(161, "utf32_icelandic_ci", 0, utf32));
        list.add(new Collation(162, "utf32_latvian_ci", 0, utf32));
        list.add(new Collation(163, "utf32_romanian_ci", 0, utf32));
        list.add(new Collation(164, "utf32_slovenian_ci", 0, utf32));
        list.add(new Collation(165, "utf32_polish_ci", 0, utf32));
        list.add(new Collation(166, "utf32_estonian_ci", 0, utf32));
        list.add(new Collation(167, "utf32_spanish_ci", 0, utf32));
        list.add(new Collation(168, "utf32_swedish_ci", 0, utf32));
        list.add(new Collation(169, "utf32_turkish_ci", 0, utf32));
        list.add(new Collation(170, "utf32_czech_ci", 0, utf32));
        list.add(new Collation(171, "utf32_danish_ci", 0, utf32));
        list.add(new Collation(172, "utf32_lithuanian_ci", 0, utf32));
        list.add(new Collation(173, "utf32_slovak_ci", 0, utf32));
        list.add(new Collation(174, "utf32_spanish2_ci", 0, utf32));
        list.add(new Collation(175, "utf32_roman_ci", 0, utf32));
        list.add(new Collation(176, "utf32_persian_ci", 0, utf32));
        list.add(new Collation(177, "utf32_esperanto_ci", 0, utf32));
        list.add(new Collation(178, "utf32_hungarian_ci", 0, utf32));
        list.add(new Collation(179, "utf32_sinhala_ci", 0, utf32));
        list.add(new Collation(180, "utf32_german2_ci", 0, utf32));
        list.add(new Collation(181, "utf32_croatian_ci", 0, utf32));
        list.add(new Collation(182, "utf32_unicode_520_ci", 0, utf32));
        list.add(new Collation(183, "utf32_vietnamese_ci", 0, utf32));

        list.add(new Collation(192, "utf8_unicode_ci", 0, utf8));
        list.add(new Collation(193, "utf8_icelandic_ci", 0, utf8));
        list.add(new Collation(194, "utf8_latvian_ci", 0, utf8));
        list.add(new Collation(195, "utf8_romanian_ci", 0, utf8));
        list.add(new Collation(196, "utf8_slovenian_ci", 0, utf8));
        list.add(new Collation(197, "utf8_polish_ci", 0, utf8));
        list.add(new Collation(198, "utf8_estonian_ci", 0, utf8));
        list.add(new Collation(199, "utf8_spanish_ci", 0, utf8));
        list.add(new Collation(200, "utf8_swedish_ci", 0, utf8));
        list.add(new Collation(201, "utf8_turkish_ci", 0, utf8));
        list.add(new Collation(202, "utf8_czech_ci", 0, utf8));
        list.add(new Collation(203, "utf8_danish_ci", 0, utf8));
        list.add(new Collation(204, "utf8_lithuanian_ci", 0, utf8));
        list.add(new Collation(205, "utf8_slovak_ci", 0, utf8));
        list.add(new Collation(206, "utf8_spanish2_ci", 0, utf8));
        list.add(new Collation(207, "utf8_roman_ci", 0, utf8));
        list.add(new Collation(208, "utf8_persian_ci", 0, utf8));
        list.add(new Collation(209, "utf8_esperanto_ci", 0, utf8));
        list.add(new Collation(210, "utf8_hungarian_ci", 0, utf8));
        list.add(new Collation(211, "utf8_sinhala_ci", 0, utf8));
        list.add(new Collation(212, "utf8_german2_ci", 0, utf8));
        list.add(new Collation(213, "utf8_croatian_ci", 0, utf8));
        list.add(new Collation(214, "utf8_unicode_520_ci", 0, utf8));
        list.add(new Collation(215, "utf8_vietnamese_ci", 0, utf8));

        list.add(new Collation(223, "utf8_general_mysql500_ci", 0, utf8));
        list.add(new Collation(224, "utf8mb4_unicode_ci", 0, utf8mb4));
        list.add(new Collation(225, "utf8mb4_icelandic_ci", 0, utf8mb4));
        list.add(new Collation(226, "utf8mb4_latvian_ci", 0, utf8mb4));
        list.add(new Collation(227, "utf8mb4_romanian_ci", 0, utf8mb4));
        list.add(new Collation(228, "utf8mb4_slovenian_ci", 0, utf8mb4));
        list.add(new Collation(229, "utf8mb4_polish_ci", 0, utf8mb4));
        list.add(new Collation(230, "utf8mb4_estonian_ci", 0, utf8mb4));
        list.add(new Collation(231, "utf8mb4_spanish_ci", 0, utf8mb4));
        list.add(new Collation(232, "utf8mb4_swedish_ci", 0, utf8mb4));
        list.add(new Collation(233, "utf8mb4_turkish_ci", 0, utf8mb4));
        list.add(new Collation(234, "utf8mb4_czech_ci", 0, utf8mb4));
        list.add(new Collation(235, "utf8mb4_danish_ci", 0, utf8mb4));
        list.add(new Collation(236, "utf8mb4_lithuanian_ci", 0, utf8mb4));
        list.add(new Collation(237, "utf8mb4_slovak_ci", 0, utf8mb4));
        list.add(new Collation(238, "utf8mb4_spanish2_ci", 0, utf8mb4));
        list.add(new Collation(239, "utf8mb4_roman_ci", 0, utf8mb4));
        list.add(new Collation(240, "utf8mb4_persian_ci", 0, utf8mb4));
        list.add(new Collation(241, "utf8mb4_esperanto_ci", 0, utf8mb4));
        list.add(new Collation(242, "utf8mb4_hungarian_ci", 0, utf8mb4));
        list.add(new Collation(243, "utf8mb4_sinhala_ci", 0, utf8mb4));
        list.add(new Collation(244, "utf8mb4_german2_ci", 0, utf8mb4));
        list.add(new Collation(245, "utf8mb4_croatian_ci", 0, utf8mb4));
        list.add(new Collation(246, "utf8mb4_unicode_520_ci", 0, utf8mb4));
        list.add(new Collation(247, "utf8mb4_vietnamese_ci", 0, utf8mb4));
        list.add(new Collation(248, "gb18030_chinese_ci", 1, gb18030));
        list.add(new Collation(249, "gb18030_bin", 0, gb18030));
        list.add(new Collation(250, "gb18030_unicode_520_ci", 0, gb18030));

        list.add(new Collation(MYSQL_COLLATION_INDEX_utf8mb4, "utf8mb4_0900_ai_ci", 1, utf8mb4));
        list.add(new Collation(256, "utf8mb4_de_pb_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(257, "utf8mb4_is_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(258, "utf8mb4_lv_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(259, "utf8mb4_ro_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(260, "utf8mb4_sl_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(261, "utf8mb4_pl_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(262, "utf8mb4_et_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(263, "utf8mb4_es_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(264, "utf8mb4_sv_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(265, "utf8mb4_tr_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(266, "utf8mb4_cs_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(267, "utf8mb4_da_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(268, "utf8mb4_lt_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(269, "utf8mb4_sk_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(270, "utf8mb4_es_trad_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(271, "utf8mb4_la_0900_ai_ci", 0, utf8mb4));

        list.add(new Collation(273, "utf8mb4_eo_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(274, "utf8mb4_hu_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(275, "utf8mb4_hr_0900_ai_ci", 0, utf8mb4));

        list.add(new Collation(277, "utf8mb4_vi_0900_ai_ci", 0, utf8mb4));

        list.add(new Collation(278, "utf8mb4_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(279, "utf8mb4_de_pb_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(280, "utf8mb4_is_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(281, "utf8mb4_lv_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(282, "utf8mb4_ro_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(283, "utf8mb4_sl_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(284, "utf8mb4_pl_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(285, "utf8mb4_et_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(286, "utf8mb4_es_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(287, "utf8mb4_sv_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(288, "utf8mb4_tr_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(289, "utf8mb4_cs_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(290, "utf8mb4_da_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(291, "utf8mb4_lt_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(292, "utf8mb4_sk_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(293, "utf8mb4_es_trad_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(294, "utf8mb4_la_0900_as_cs", 0, utf8mb4));

        list.add(new Collation(296, "utf8mb4_eo_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(297, "utf8mb4_hu_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(298, "utf8mb4_hr_0900_as_cs", 0, utf8mb4));

        list.add(new Collation(300, "utf8mb4_vi_0900_as_cs", 0, utf8mb4));

        list.add(new Collation(303, "utf8mb4_ja_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(304, "utf8mb4_ja_0900_as_cs_ks", 0, utf8mb4));
        list.add(new Collation(305, "utf8mb4_0900_as_ci", 0, utf8mb4));
        list.add(new Collation(306, "utf8mb4_ru_0900_ai_ci", 0, utf8mb4));
        list.add(new Collation(307, "utf8mb4_ru_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(308, "utf8mb4_zh_0900_as_cs", 0, utf8mb4));
        list.add(new Collation(309, "utf8mb4_0900_bin", 0, utf8mb4));

        list.add(new Collation(326, "utf8mb4_test_ci", 0, utf8mb4));
        list.add(new Collation(327, "utf16_test_ci", 0, utf16));
        list.add(new Collation(328, "utf8mb4_test_400_ci", 0, utf8mb4));

        list.add(new Collation(336, "utf8_bengali_standard_ci", 0, utf8));
        list.add(new Collation(337, "utf8_bengali_traditional_ci", 0, utf8));

        list.add(new Collation(352, "utf8_phone_ci", 0, utf8));
        list.add(new Collation(353, "utf8_test_ci", 0, utf8));
        list.add(new Collation(354, "utf8_5624_1", 0, utf8));
        list.add(new Collation(355, "utf8_5624_2", 0, utf8));
        list.add(new Collation(356, "utf8_5624_3", 0, utf8));
        list.add(new Collation(357, "utf8_5624_4", 0, utf8));
        list.add(new Collation(358, "ucs2_test_ci", 0, ucs2));
        list.add(new Collation(359, "ucs2_vn_ci", 0, ucs2));
        list.add(new Collation(360, "ucs2_5624_1", 0, ucs2));

        list.add(new Collation(368, "utf8_5624_5", 0, utf8));
        list.add(new Collation(391, "utf32_test_ci", 0, utf32));
        list.add(new Collation(2047, "utf8_maxuserid_ci", 0, utf8));

        return Collections.unmodifiableList(list);
    }


    /**
     * @return a unmodifiable map
     */
    private static Map<Integer, Collation> createCollationMap() {
        List<Collation> collationList = createCollationList();
        Map<Integer, Collation> map = MySQLCollections.hashMap((int) (collationList.size() / 0.75f));
        for (Collation collation : collationList) {
            map.put(collation.index, collation);
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * @return a unmodifiable collection
     * @see #UNSUPPORTED_CHARSET_CLIENTS
     * @see #isUnsupportedCharsetClient(String)
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html#charset-connection-impermissible-client-charset">Impermissible Client Character Sets</a>
     */
    private static Collection<String> createUnsupportedCharsetClients() {
        Set<String> set = new HashSet<>();

        set.add(StandardCharsets.UTF_16.name());
        set.addAll(StandardCharsets.UTF_16.aliases());
        set.add(StandardCharsets.UTF_16LE.name());
        set.addAll(StandardCharsets.UTF_16LE.aliases());

        set.add("utf32");
        set.add("utf-32");
        set.add("UTF32");
        set.add("UTF-32");

        set.add("ucs2");
        set.add("ucs-2");
        set.add("UCS2");
        set.add("UCS-2");

        final String keyText = "\0'\032;\\";
        final byte[] keyBytes = keyText.getBytes(StandardCharsets.US_ASCII);
        byte[] bytes;
        for (Charset charset : Charset.availableCharsets().values()) {
            try {
                bytes = keyText.getBytes(charset);
                if (!Arrays.equals(bytes, keyBytes)) {
                    set.add(charset.name());
                    set.addAll(charset.aliases());
                }
            } catch (Throwable e) {
                set.add(charset.name());
                set.addAll(charset.aliases());
            }
        }
        return Collections.unmodifiableCollection(set);
    }


    /*################################## blow static class ##################################*/

    public static final class CustomCollation {

        public final int index;

        public final String collationName;

        public final String charsetName;

        public final int maxLen;

        public CustomCollation(int index, String collationName, String charsetName, int maxLen) {
            this.index = index;
            this.collationName = collationName;
            this.charsetName = charsetName;
            this.maxLen = maxLen;
        }
    }
}
