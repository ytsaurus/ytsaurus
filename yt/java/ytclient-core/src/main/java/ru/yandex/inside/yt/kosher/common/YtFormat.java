package ru.yandex.inside.yt.kosher.common;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeStringNode;

/**
 * @author sankear
 */
public final class YtFormat {

    public static final YTreeStringNode YSON_BINARY = YTree.builder()
            .beginAttributes()
                .key("format").value("binary")
            .endAttributes()
            .value("yson")
            .build()
            .stringNode();

    public static final YTreeStringNode YSON_TEXT = YTree.builder()
            .beginAttributes()
                .key("format").value("text")
            .endAttributes()
            .value("yson")
            .build()
            .stringNode();

    public static final YTreeStringNode YAMR = YTree.stringNode("yamr");

    public static final YTreeStringNode YAMR_SUBKEY = YTree.builder()
            .beginAttributes()
                .key("has_subkey").value(true)
            .endAttributes()
            .value("yamr")
            .build()
            .stringNode();

    public static final YTreeStringNode DSV = YTree.stringNode("dsv");

    public static final YTreeStringNode JSON = YTree.builder()
                    .beginAttributes()
                        .key("encode_utf8").value(false)
                    .endAttributes()
                    .value("json")
                    .build()
                    .stringNode();

    private YtFormat() {

    }

}
