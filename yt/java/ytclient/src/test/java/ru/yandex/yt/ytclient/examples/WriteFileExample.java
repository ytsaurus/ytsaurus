package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.misc.ExceptionUtils;
import ru.yandex.yt.ytclient.proxy.FileWriter;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;

import java.io.FileInputStream;

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
                        .setWindowSize(16000000L)
                        .setPacketSize(1000000L)
                        .setComputeMd5(true)
                ).join();

                FileInputStream fi = new FileInputStream("test.txt");
                byte[] data = new byte[40960];
                int size;
                while ((size = fi.read(data)) > 0) {
                    writer.write(data, 0, size);
                }
                writer.close();
                fi.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

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

                writer.write(() -> {
                    try {
                        int size = fi.read(data);
                        if (size <= 0) {
                            return null;
                        }

                        byte[] newdata = new byte[size];
                        System.arraycopy(data, 0, newdata, 0, size);
                        return newdata;
                    } catch (Exception ex) {
                        throw ExceptionUtils.translate(ex);
                    }
                }).join();

                fi.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

        ExamplesUtil.enableCompression();

        ExamplesUtil.runExample(client -> {
            try {
                logger.info("Write file 3");

                String path = "//tmp/bigfile3";
                client.createNode(new CreateNode(path, ObjectType.File).setForce(true)).join();
                FileWriter writer = client.writeFile(new WriteFile(path)
                        .setWindowSize(16000000L)
                        .setPacketSize(1000000L)
                        .setComputeMd5(true)
                ).join();

                FileInputStream fi = new FileInputStream("test.txt");

                byte[] data = new byte[40960];

                writer.write(() -> {
                    try {
                        int size = fi.read(data);
                        if (size <= 0) {
                            return null;
                        }

                        byte[] newdata = new byte[size];
                        System.arraycopy(data, 0, newdata, 0, size);
                        return newdata;
                    } catch (Exception ex) {
                        throw ExceptionUtils.translate(ex);
                    }
                }).join();

                fi.close();

            } catch (Throwable e) {
                logger.error("Error {}", e);
                System.exit(0);
            }
        });

    }

}
