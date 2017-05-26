package ru.yandex.yt.ytclient.ytree;

public final class YTreeUtil {
    private YTreeUtil() {
        // static class
    }

    public static String stringValue(YTreeNode node, String defaultValue) {
        return node != null ? node.stringValue() : defaultValue;
    }

    public static byte[] bytesValue(YTreeNode node, byte[] defaultValue) {
        return node != null ? node.bytesValue() : defaultValue;
    }

    public static long longValue(YTreeNode node, long defaultValue) {
        return node != null ? node.longValue() : defaultValue;
    }

    public static int intValue(YTreeNode node, int defaultValue) {
        return node != null ? node.intValue() : defaultValue;
    }

    public static double doubleValue(YTreeNode node, double defaultValue) {
        return node != null ? node.doubleValue() : defaultValue;
    }

    public static boolean booleanValue(YTreeNode node, boolean defaultValue) {
        return node != null ? node.booleanValue() : defaultValue;
    }
}
