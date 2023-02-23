package tech.ytsaurus.typeinfo;

import java.util.HashMap;
import java.util.Map;

class Escape {
    private static final Map<Character, String> ESCAPE_MAP = new HashMap<>();

    private Escape() {
    }

    static String quote(String value) {
        if (value.isEmpty()) {
            return "''";
        }
        int valueLength = value.length();
        int curIndex = 0;
        for (; curIndex < valueLength; ++curIndex) {
            if (needEscaping(value.charAt(curIndex))) {
                break;
            }
        }
        if (curIndex == valueLength) {
            return "'" + value + "'";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("'");
        sb.append(value, 0, curIndex);
        for (; curIndex < valueLength; ++curIndex) {
            char c = value.charAt(curIndex);
            escape(c, sb);
        }
        return sb.toString();
    }

    private static void escape(char c, StringBuilder sb) {
        if (needEscaping(c)) {
            String escaped = ESCAPE_MAP.get(c);
            if (escaped == null) {
                sb.append("\\u").append(String.format("%04x", (int) c));
            } else {
                sb.append(escaped);
            }
        } else {
            sb.append(c);
        }
    }

    private static boolean needEscaping(char c) {
        return c == '\'' ||
                c == '\t' ||
                c == '\n' ||
                c == '\r' ||
                c == '\\' ||
                Character.isISOControl(c);
    }

    static {
        ESCAPE_MAP.put('\'', "\\'");
        ESCAPE_MAP.put('\t', "\\t");
        ESCAPE_MAP.put('\n', "\\n");
        ESCAPE_MAP.put('\r', "\\r");
        ESCAPE_MAP.put('\\', "\\\\");
    }
}
