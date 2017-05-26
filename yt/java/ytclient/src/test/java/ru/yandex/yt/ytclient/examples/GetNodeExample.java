package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetNodeExample {
    private static final Logger logger = LoggerFactory.getLogger(GetNodeExample.class);

    public static void main(String[] args) {
        ExamplesUtil.runExample(client -> {
            logger.info("Table dynamic: {}", client.getNode("//yabs/GPStat3.dynamic/@dynamic").join());
            logger.info("Table schema: {}", client.getNode("//yabs/GPStat3.dynamic/@schema").join());
        });
    }
}
