package ru.yandex.yt.ytclient.examples;

import java.io.FileOutputStream;

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
                FileReader reader = client.readFile(new ReadFile("//tmp/bigfile"));

                FileOutputStream fo = new FileOutputStream("test.txt");

                byte [] data;
                while ((data = reader.read()) != null) {
                    fo.write(data);
                }

                fo.close();

                reader.waitResult().get();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });


        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Read file");
                FileReader reader = client.readFile(new ReadFile("//tmp/bigfile"));

                FileOutputStream fo = new FileOutputStream("test2.txt");

                reader.read((data) -> {
                    try {
                        fo.write(data);
                    } catch (Exception ex) {
                        throw ExceptionUtils.translate(ex);
                    }
                });

                reader.waitResult().get();

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
                FileReader reader = client.readFile(new ReadFile("//tmp/bigfile"));

                FileOutputStream fo = new FileOutputStream("test3.txt");

                byte [] data;
                while ((data = reader.read()) != null) {
                    fo.write(data);
                }

                fo.close();

                reader.waitResult().get();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

    }
}
