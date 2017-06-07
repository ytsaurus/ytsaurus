package ru.yandex.yt.rpc.utils;

import java.util.List;

/**
 * @author valri
 */
public class Utility {
    public static byte[] byteArrayFromList(List<Byte> list) {
        final byte[] result = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i);
        }
        return result;
    }

    public static int toInt(byte[] bytes, int offset) {
        int ret = 0;
        for (int i = 0; i < 4 && i + offset < bytes.length; i++) {
            ret <<= 8;
            ret |= (int) bytes[offset + i] & 0xFF;
        }
        return ret;
    }
}
