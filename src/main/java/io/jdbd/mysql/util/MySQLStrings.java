package io.jdbd.mysql.util;

import io.jdbd.vendor.util.JdbdStrings;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Stack;

public abstract class MySQLStrings extends JdbdStrings {



    public static byte[] getBytesNullTerminated(String text, Charset charset) {
        byte[] textBytes = text.getBytes(charset);
        byte[] bytes = new byte[textBytes.length + 1];

        System.arraycopy(textBytes, 0, bytes, 0, textBytes.length);
        bytes[textBytes.length] = 0;
        return bytes;
    }

    public static boolean isBinaryString(String text) {
        final int length = text.length();
        char ch;
        for (int i = 0; i < length; i++) {
            ch = text.charAt(i);
            if (ch != '0' && ch != '1') {
                return false;
            }
        }
        return true;
    }

    public static byte[] binaryStringToBytes(final char[] binaryString) {
        final byte[] bytes = new byte[(binaryString.length + 7) >> 3];
        char ch;
        for (int i = 0; i < binaryString.length; i++) {
            ch = binaryString[i];
            if (ch == '1') {
                bytes[i >> 3] |= (1 << (i & 7));
            } else if (ch != '0') {
                throw new IllegalArgumentException("binaryString isn't binary string.");
            }
        }
        return bytes;
    }

    public static String trimTrailingSpace(final String text) {
        String newText = null;
        final int length = text.length();
        for (int i = length - 1; i > -1; i--) {
            char ch = text.charAt(i);
            if (ch != ' ') {
                newText = text.substring(0, i + 1);
                break;
            }
        }
        return newText == null ? "" : newText;
    }

    /**
     * Splits input into a list, using the given delimiter and skipping all between the given markers.
     * <p>
     * note:openMarker[i] and openMarker[i] is match.
     * </p>
     *
     * @param input       the string to split
     * @param openMarker  the string to split on
     * @param closeMarker characters which delimit the beginning of a text block to skip
     * @return the  list of strings, split by delimiter, maybe empty.
     * @throws IllegalArgumentException if an error occurs
     */
    public static List<String> split(String input, String delimiter, String openMarker, String closeMarker) {

        final char[] delimiterArray = Objects.requireNonNull(delimiter, "delimiter").toCharArray();
        final char[] openMarkerArray = Objects.requireNonNull(openMarker, "openMarker").toCharArray();
        final char[] closeMarkerArray = Objects.requireNonNull(closeMarker, "closeMarker").toCharArray();

        if (openMarkerArray.length != closeMarkerArray.length) {
            throw new IllegalArgumentException(String.format
                    ("openMarker[%s] and closeMarker[%s] not match.", openMarker, closeMarker));
        }

        final Stack<Character> openMarkerStack = new Stack<>();
        List<String> list = new ArrayList<>();

        final int size = Objects.requireNonNull(input, "input").length();
        int start = 0;
        char current, lastOpenMarker;
        for (int i = 0, openMarkerIndex, closeMarkerIndex, charCount = 0; i < size; i++) {
            current = input.charAt(i);
            if (Character.isWhitespace(current)) {
                continue;
            }
            charCount++;

            openMarkerIndex = indexMarker(current, openMarkerArray);
            closeMarkerIndex = indexMarker(current, closeMarkerArray);

            if (openMarkerIndex > -1 && (openMarkerStack.isEmpty()) | closeMarkerIndex < 0) {
                // current is open marker
                openMarkerStack.push(current);
                continue;
            } else if (openMarkerIndex > -1) {
                // current is both open marker and close marker . and openMarkerStack not empty
                lastOpenMarker = openMarkerStack.peek();
                if (lastOpenMarker == current) {
                    openMarkerStack.pop();
                } else {
                    openMarkerStack.push(current);
                }
                continue;
            } else if (closeMarkerIndex > -1) {
                // current is just close marker
                if (openMarkerStack.isEmpty()) {
                    throw createFormatException(input, i);
                }
                lastOpenMarker = openMarkerStack.peek();
                openMarkerIndex = indexMarker(lastOpenMarker, openMarkerArray);
                if (openMarkerIndex != closeMarkerIndex) {
                    throw createFormatException(input, i);
                }
                // marker match
                openMarkerStack.pop();
                continue;
            }
            // current is neither open marker nor close marker.
            if (!openMarkerStack.isEmpty()) {
                continue;
            }
            if (isDelimiter(current, delimiterArray)) {
                charCount--; //skip delimiter
                if (charCount == 0) {
                    throw createFormatException(input, i);
                }
                list.add(input.substring(start, i).trim());
                start = i + 1;
                charCount = 0;
            }

        }
        if (!openMarkerStack.isEmpty()) {
            throw new IllegalArgumentException(String.format("[%s] not close marker", input));
        }
        if (start < size) {
            list.add(input.substring(start));
        }
        return list;
    }


    /*################################## blow private static method ##################################*/


    /**
     * @see #split(String, String, String, String)
     */
    private static boolean isDelimiter(char current, char[] delimiterArray) {
        for (char c : delimiterArray) {
            if (c == current) {
                return true;
            }
        }
        return false;
    }

    /**
     * @see #split(String, String, String, String)
     */
    private static int indexMarker(char current, final char[] markerArray) {

        for (int i = 0; i < markerArray.length; i++) {
            if (current == markerArray[i]) {
                return i;
            }
        }
        return -1;
    }

    private static IllegalArgumentException createFormatException(String input, int currentIndex) {
        final int len = input.length();
        if (currentIndex < 0 || currentIndex >= len) {
            throw new IllegalArgumentException("currentIndex error");
        }
        int start = currentIndex - 10;
        int end = currentIndex + 10;
        if (start < 0) {
            start = 0;
        }
        if (end >= len) {
            end = len;
        }
        throw new IllegalArgumentException(
                String.format("Index[%s] Char[%s] nearby[%s] format error."
                        , currentIndex, input.charAt(currentIndex), input.substring(start, end)));
    }

}
