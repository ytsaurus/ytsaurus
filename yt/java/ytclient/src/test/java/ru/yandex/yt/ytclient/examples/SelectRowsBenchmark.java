package ru.yandex.yt.ytclient.examples;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.yt.ytclient.bus.BusConnector;
import ru.yandex.yt.ytclient.proxy.ApiServiceClient;
import ru.yandex.yt.ytclient.rpc.BalancingRpcClient;
import ru.yandex.yt.ytclient.rpc.RpcClient;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.wire.UnversionedRowset;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by aozeritsky on 26.05.2017.
 */
public class SelectRowsBenchmark {
    private static final Logger logger = LoggerFactory.getLogger(SelectRowsExample.class);

    // runme: --proxy n0035-myt.seneca-myt.yt.yandex.net,n0036-myt.seneca-myt.yt.yandex.net,n0037-myt.seneca-myt.yt.yandex.net --input requests
    public static void main(String[] args) throws Exception {
        final BusConnector connector = ExamplesUtil.createConnector();
        final String user = ExamplesUtil.getUser();
        String token = ExamplesUtil.getToken();

        final MetricRegistry metrics = new MetricRegistry();
        final Histogram metric = metrics.histogram("requests");

        OptionParser parser = new OptionParser();

        OptionSpec<String> proxyOpt = parser.accepts("proxy", "proxy")
            .withRequiredArg().ofType(String.class).withValuesSeparatedBy(',');
        OptionSpec<String> tokenOpt = parser.accepts("token", "token")
            .withRequiredArg().ofType(String.class);
        OptionSpec<String> inputOpt = parser.accepts("input", "input")
            .withRequiredArg().ofType(String.class);
        OptionSpec<Integer> switchTimeoutOpt = parser.accepts("switchtimeout", "switchtimeout")
            .withRequiredArg().ofType(Integer.class);

        List<String> proxies = null;
        final List<String> requests = new ArrayList<>();
        Duration localTimeout = Duration.ofMillis(60);

        OptionSet option = parser.parse(args);

        if (option.hasArgument(tokenOpt)) {
            token = option.valueOf(tokenOpt);
        }

        if (option.hasArgument(proxyOpt)) {
            proxies = option.valuesOf(proxyOpt);
        } else {
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        if (option.hasArgument(inputOpt)) {
            Stream<String> lines = Files.lines(Paths.get(option.valueOf(inputOpt)));
            lines.forEach(line -> {
                String newLine = line.trim();
                if (!newLine.isEmpty()) {
                    requests.add(newLine);
                }
            });
        } else {
            parser.printHelpOn(System.out);
            System.exit(1);
        }

        if (option.hasArgument(switchTimeoutOpt)) {
            localTimeout = Duration.ofMillis(option.valueOf(switchTimeoutOpt));
        }

        final String finalToken = token;

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
        reporter.start(5, TimeUnit.SECONDS);

        List<RpcClient> proxiesConnections = proxies.stream().map(x ->
            ExamplesUtil.createRpcClient(connector, user, finalToken, x, 9013)
        ).collect(Collectors.toList());

        RpcClient rpcClient = new BalancingRpcClient(
            localTimeout,
            proxiesConnections
        );

        ApiServiceClient client = new ApiServiceClient(rpcClient,
            new RpcOptions().setDefaultTimeout(Duration.ofSeconds(5)));

        for (;;) {
            try {
                for (String request : requests) {
                    long t0 = System.nanoTime();
                    UnversionedRowset rowset = client.selectRows(request).join();
                    long t1 = System.nanoTime();
                    metric.update((t1 - t0) / 1000000);
                    // logger.info("Request time: {}", (t1 - t0) / 1000000.0);
                }
            } catch (Throwable e) {
                logger.error("error `{}`", e.toString());
            }
        }
    }
}
