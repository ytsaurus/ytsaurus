package ru.yandex.yt.ytclient.examples;

import java.io.FileOutputStream;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.misc.ExceptionUtils;
import ru.yandex.yt.ytclient.proxy.FileReader;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;

public class ReadFileExample {
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
                logger.info("Read file");
                FileReader reader = client.readFile(new ReadFile("//tmp/bigfile")).join();

                FileOutputStream fo = new FileOutputStream("test.txt");

                byte [] data;
                while ((data = reader.read()) != null) {
                    fo.write(data);
                }

                fo.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });


        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Read file");
                FileReader reader = client.readFile(new ReadFile("//tmp/bigfile")).join();

                FileOutputStream fo = new FileOutputStream("test2.txt");

                reader.read((data) -> {
                    try {
                        if (data != null) {
                            fo.write(data);
                        }
                    } catch (Exception ex) {
                        throw ExceptionUtils.translate(ex);
                    }
                }).get();

                fo.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

        ExamplesUtil.enableCompression();

        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Read file 3");
                FileReader reader = client.readFile(new ReadFile("//tmp/bigfile")).join();

                FileOutputStream fo = new FileOutputStream("test3.txt");

                byte [] data;
                while ((data = reader.read()) != null) {
                    fo.write(data);
                }

                fo.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Read file 3");
                FileReader reader = client.readFile(new ReadFile("//tmp/badfile-" + UUID.randomUUID().toString())).join();

                FileOutputStream fo = new FileOutputStream("test4.txt");

                byte [] data;
                while ((data = reader.read()) != null) {
                    fo.write(data);
                }

                fo.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

    }
}
