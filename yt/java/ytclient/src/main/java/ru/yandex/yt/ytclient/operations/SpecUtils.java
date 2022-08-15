package ru.yandex.yt.ytclient.operations;


import java.net.InetAddress;
import java.net.UnknownHostException;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;

final class SpecUtils {
    private SpecUtils() {
    }

    static YTreeBuilder startedBy(YTreeBuilder builder) {
        return builder.beginMap()
                .key("user").value(System.getProperty("user.name"))
                .key("command").beginList().value("command").endList()
                .key("hostname").value(getLocalHostname())
                //.key("pid").value(ProcessUtils.getPid()) TODO
                //.key("wrapper_version").value(YtConfiguration.getVersion())
                .endMap();
    }

    private static String getLocalHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
}
