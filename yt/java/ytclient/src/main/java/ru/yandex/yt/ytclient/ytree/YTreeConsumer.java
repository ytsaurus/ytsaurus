package ru.yandex.yt.ytclient.ytree;

import java.util.List;
import java.util.Map;

public interface YTreeConsumer {
    void onStringScalar(String value);

    void onStringScalar(byte[] value);

    void onInt64Scalar(long value);

    void onUint64Scalar(long value);

    void onDoubleScalar(double value);

    void onBooleanScalar(boolean value);

    void onEntity();

    void onBeginList();

    void onListItem();

    void onEndList();

    void onBeginMap();

    void onKeyedItem(String key);

    void onEndMap();

    void onBeginAttributes();

    void onEndAttributes();

    default boolean isBinaryPreferred() {
        return true;
    }

    default void onListFragment(List<? extends YTreeNode> list) {
        for (YTreeNode value : list) {
            onListItem();
            if (value != null) {
                value.writeTo(this);
            } else {
                onEntity();
            }
        }
    }

    default void onMapFragment(Map<String, ? extends YTreeNode> map) {
        for (Map.Entry<String, ? extends YTreeNode> entry : map.entrySet()) {
            String key = entry.getKey();
            YTreeNode value = entry.getValue();
            onKeyedItem(key);
            if (value != null) {
                value.writeTo(this);
            } else {
                onEntity();
            }
        }
    }
}
