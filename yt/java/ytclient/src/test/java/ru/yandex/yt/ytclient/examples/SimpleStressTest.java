package ru.yandex.yt.ytclient.examples;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.misc.YtTimestamp;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.proxy.LookupRowsRequest;
import ru.yandex.yt.ytclient.proxy.ModifyRowsRequest;
import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;
import ru.yandex.yt.ytclient.rpc.DefaultRpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

public class SimpleStressTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleStressTest.class);
    private static String user = ExamplesUtil.getUser();
    private static String token = ExamplesUtil.getToken();
    private Map<String, List<String>> proxies = null;

    private final String tablePath = "//home/stress-test";
    private final TableSchema schema = new TableSchema.Builder()
            .addKey("HostHash", ColumnValueType.UINT64)
            .addKey("UrlHashFirst", ColumnValueType.UINT64)
            .addKey("UrlHashSecond", ColumnValueType.UINT64)
            .addValue("Timestamp", ColumnValueType.UINT64)
            .addValue("Data", ColumnValueType.STRING)
            .build();

    private final int threads = 32;
    private final ExecutorService executorService = Executors.newFixedThreadPool(threads);

    static class Data {
        long h1;
        long h2;
        long h3;
        long h4;
        byte [] bytes;
        boolean found = false;

        Data(long id, long j, long max, Random rnd) {
            h1 = id << 32 | j;
            h2 = id << 32 | (j + max);
            h3 = id << 32 | (j + 2 * max);
            h4 = id << 32 | (j + 3 * max);
            bytes = new byte[1024];
            rnd.nextBytes(bytes);
        }

        Data(long h1, long h2, long h3, long h4, byte[] bytes) {
            this.h1 = h1;
            this.h2 = h2;
            this.h3 = h3;
            this.h4 = h4;
            this.bytes = bytes;
        }

        static Data fromYTreeNode(YTreeMapNode node) {
            return new Data(
                node.getLong("HostHash"),
                node.getLong("UrlHashFirst"),
                node.getLong("UrlHashSecond"),
                node.getLong("Timestamp"),
                node.getBytes("Data"));
        }

        @Override
        public boolean equals(Object other) {
            Data d2 = (Data)other;
            return
                h1 == d2.h1 &&
                h2 == d2.h2 &&
                h3 == d2.h3 &&
                h4 == d2.h4 &&
                Arrays.equals(bytes, d2.bytes);
        }
    }

    List<Data> generateData(long id, long it, long max, Random rnd) {
        List<Data> result = new ArrayList<>();
        for (long j = 0; j < max; ++j) {
            result.add(new Data(id, it + j, max, rnd));
        }
        return result;
    }

    void insertData(List<Data> data, BalancingRpcClient balancingRpcClient) throws Throwable {
        int retry;
        final int maxRetries = 10;

        for (retry = 0; retry < maxRetries; ++retry) {
            try {
                RpcClient rpcClient = balancingRpcClient.getAliveClient();
                ApiServiceClient serviceClient = new ApiServiceClient(rpcClient,
                        new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));
                ApiServiceTransactionOptions transactionOptions =
                        new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                                .setSticky(true);


                ApiServiceTransaction transaction = serviceClient.startTransaction(transactionOptions).join();

                ModifyRowsRequest request = new ModifyRowsRequest(tablePath, schema);

                for (Data d : data) {
                    request.addUpdate(Arrays.asList(d.h1, d.h2, d.h3, d.h4, d.bytes));
                }

                transaction.modifyRows(request).join();
                transaction.commit().join();

                break;
            } catch (Throwable e) {
                e.printStackTrace();
                System.err.println(String.format("retry = %d", retry));
                Thread.sleep(1000);
            }
        }

        if (retry == maxRetries) {
            System.err.println("max retries reached");
            System.exit(1);
        }
    }

    void checkData(List<Data> data, ApiServiceClient client) throws Throwable {
        LookupRowsRequest request = new LookupRowsRequest(tablePath, schema.toLookup());

        for (Data d : data) {
            request.addFilter(d.h1, d.h2, d.h3);
        }

        Map<Long, Data> hash = new HashMap<>();
        for (Data d : data) {
            hash.put(d.h1, d);
        }

        int retry;
        int maxRetries = 10;
        for (retry = 0; retry < maxRetries; ++retry) {
            try {
                UnversionedRowset rowset = client.lookupRows(request, YtTimestamp.SYNC_LAST_COMMITTED).join();
                for (YTreeMapNode row : rowset.getYTreeRows()) {
                    if (row == null) {
                        continue;
                    }
                    Data d = Data.fromYTreeNode(row);
                    Data d2 = hash.get(d.h1);
                    d2.found = true;
                    if (!d.equals(d2)) {
                        System.err.println("d != d2");
                        System.exit(-1);
                    }
                }

                for (Map.Entry<Long, Data> entry : hash.entrySet()) {
                    Data d = entry.getValue();
                    if (!d.found) {
                        System.err.println(String.format("not found %d %d %d %d", d.h1, d.h2, d.h3, d.h4));
                    }
                }

                break;
            } catch (Throwable e) {
                e.printStackTrace();
                System.err.println(String.format("lookup retry = %d", retry));
                Thread.sleep(1000);
            }
        }

        if (retry == maxRetries) {
            System.err.println("max lookup retries reached");
            System.exit(1);
        }
    }

    void run(String[] args) throws Throwable
    {
        OptionParser parser = new OptionParser();
        OptionSpec<String> proxyOpt = parser.accepts("proxy", "proxy")
                .withRequiredArg().ofType(String.class);

        OptionSet option = parser.parse(args);

        if (option.hasArgument(proxyOpt)) {
            String line = option.valueOf(proxyOpt);
            File f = new File(line);
            if (f.exists() && !f.isDirectory()) {
                Stream<String> stream = Files.lines(f.toPath());
                proxies = new HashMap<>();
                ArrayList<String> list = new ArrayList<>();

                stream.forEach(
                    l -> {
                        l.trim();
                        if (!l.isEmpty()) {
                            list.add(l);
                        }
                    }
                );

                proxies.put("unknown", list);

            } else {
                // n0035-myt.seneca-myt.yt.yandex.net,n0036-myt.seneca-myt.yt.yandex.net,n0037-myt.seneca-myt.yt.yandex.net
                // myt:n0035-myt.seneca-myt.yt.yandex.net,n0036-myt.seneca-myt.yt.yandex.net,n0037-myt.seneca-myt.yt.yandex.net;sas:n0035-sas.seneca-myt.yt.yandex.net
                if (line.indexOf(':') == -1) {
                    // simple format
                    proxies = ImmutableMap.of("unknown", Arrays.asList(line.split(",")));
                } else {
                    proxies = new HashMap<>();
                    for (String part : line.split(";")) {
                        String[] dcAndProxies = part.split(":");
                        String dc = dcAndProxies[0];
                        proxies.put(dc, Arrays.asList(dcAndProxies[1].split(",")));
                    }
                }
            }
        } else {
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        final BusConnector connector;
        final ApiServiceClient client;

        connector = ExamplesUtil.createConnector(16);

        Map<String, List<RpcClient>> proxiesConnections = proxies.entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> e.getKey(),
                    e ->
                        e.getValue().stream().map(x ->
                            ExamplesUtil.createRpcClient(
                                connector, user, token, x, 9013, e.getKey()))
                            .collect(Collectors.toList())
                ));

        BalancingRpcClient balancingRpcClient = new BalancingRpcClient(
                Duration.ofMillis(60000),
                Duration.ofMillis(60000),
                Duration.ofMillis(60000),
                connector,
                new DefaultRpcFailoverPolicy(),
                "unknown",
                proxiesConnections
        );

        client = new ApiServiceClient(balancingRpcClient,
                new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));

        Thread.sleep(10000);
        for (int i = 0; i < threads; ++i) {
            long id = i;
            executorService.execute(() -> {
                Random rnd = new Random();
                long max = 100;
                for (long it = 0;; it += max) {
                    try {
                        List<Data> data = generateData(id, it, max, rnd);
                        System.err.println(String.format("insertData: %d %d", id, it));
                        insertData(data, balancingRpcClient);
                        System.err.println(String.format("checkData: %d %d", id, it));
                        checkData(data, client);
                    } catch (Throwable e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            });
        }

        executorService.awaitTermination(10, TimeUnit.DAYS);
    }

    public static void main(String[] args) {
        try {
            SimpleStressTest test = new SimpleStressTest();
            test.run(args);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.out.println("done");
            System.exit(0);
        }
    }
}
