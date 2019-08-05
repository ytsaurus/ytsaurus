package ru.yandex.yt.ytclient.examples;

import java.io.FileInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.yt.ytclient.proxy.FileWriter;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;

public class WriteFileExample {
    private static final Logger logger = LoggerFactory.getLogger(WriteFileExample.class);

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
                logger.info("Write file");

                String path = "//tmp/bigfile1";
                client.createNode(new CreateNode(path, ObjectType.File).setForce(true)).join();
                FileWriter writer = client.writeFile(new WriteFile(path)
                        .setWindowSize(10000000L)
                        .setPacketSize(1000000L)
                        .setComputeMd5(true)
                ).join();

                FileInputStream fi = new FileInputStream("test.txt");
                byte[] data = new byte[40960];

                int size = fi.read(data);
                while (size > 0) {
                    while (size > 0 && writer.write(data, 0, size)) {
                        size = fi.read(data);
                    }

                    writer.readyEvent().join();
                }
                writer.close().join();
                fi.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

        ExamplesUtil.enableCompression();


        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Write file 2");

                String path = "//tmp/bigfile2";
                client.createNode(new CreateNode(path, ObjectType.File).setForce(true)).join();
                FileWriter writer = client.writeFile(new WriteFile(path)
                        .setWindowSize(16000000L)
                        .setPacketSize(1000000L)
                        .setComputeMd5(true)
                ).join();

                FileInputStream fi = new FileInputStream("test.txt");
                byte[] data = new byte[40960];
                int size = fi.read(data);
                while (size > 0) {
                    while (size > 0 && writer.write(data, 0, size)) {
                        size = fi.read(data);
                    }

                    writer.readyEvent().join();
                }
                writer.close().join();
                fi.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });
    }

}
