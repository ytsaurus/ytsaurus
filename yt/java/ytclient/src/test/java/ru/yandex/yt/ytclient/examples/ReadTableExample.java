package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.TableReader;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

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
                logger.info("Read table");
                TableReader reader = client.readTable(new ReadTable("//home/dev/andozer/autorestart_nodes_copy")).join();

                UnversionedRowset rowset;

                while (reader.canRead()) {
                    while ((rowset = reader.read()) != null) {
                        logger.info("rows {}", rowset.getRows().size());
                        logger.info("stat {}", reader.getDataStatistics());

                    }
                    reader.readyEvent().join();
                }

                reader.close().join();

            } catch (Throwable e) {
                logger.error("error {}", e);
                System.exit(0);
            }
        });
    }
}
