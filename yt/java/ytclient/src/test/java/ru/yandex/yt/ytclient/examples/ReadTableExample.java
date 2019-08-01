package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.internal.TableReaderImpl;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;

public class ReadTableExample {
    private static final Logger logger = LoggerFactory.getLogger(ReadTableExample.class);

    public static void main(String[] args) {
        try {
            logger.debug("Starting");
            mainUnsafe(args);
        } catch (Throwable e) {
            System.err.println(e);
            System.exit(-1);
        }
    }

    private static void mainUnsafe(String[] args) {
        ExamplesUtil.runExample(client -> {
            try {
                // Thread.sleep(5000);
                logger.info("Read table");
                //TableReader reader = client.readTable(new ReadTable("//home/market/production/mbo/export/recent/models/models"));
                TableReaderImpl reader = client.readTable(new ReadTable("//home/dev/andozer/autorestart_nodes_copy"));
                //TableReader reader = client.readTable(new ReadTable("//home/arivkin/choyt/demo/mobile_sorted_small"));

                reader.waitResult();

            } catch (Throwable e) {
                System.exit(0);
            }
        });
    }
}
