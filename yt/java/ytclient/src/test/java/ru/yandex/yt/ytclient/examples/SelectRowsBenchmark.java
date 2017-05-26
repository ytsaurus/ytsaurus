package ru.yandex.yt.ytclient.examples;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.yt.ytclient.bus.BusConnector;

/**
 * Created by aozeritsky on 26.05.2017.
 */
public class SelectRowsBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(SelectRowsExample.class);

    public static void main(String[] args) throws Exception {
        BusConnector connector = ExamplesUtil.createConnector();
        String user = ExamplesUtil.getUser();
        String token = ExamplesUtil.getToken();
    }
}
