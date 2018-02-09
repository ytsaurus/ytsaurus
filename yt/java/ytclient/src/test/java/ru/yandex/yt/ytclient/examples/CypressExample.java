package ru.yandex.yt.ytclient.examples;

import java.util.Random;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.misc.YtGuid;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;

public class CypressExample {

    public static void main(String[] args) {
        try {
            OptionParser parser = new OptionParser();

            OptionSpec<String> proxyOpt = parser.accepts("proxy", "proxy (see //sys/rpc_proxies)")
                    .withRequiredArg().ofType(String.class);

            OptionSet option = parser.parse(args);

            String [] hosts = null;

            if (option.hasArgument(proxyOpt)) {
                String line = option.valueOf(proxyOpt);
                hosts = line.split(",");
            } else {
                parser.printHelpOn(System.out);
                System.exit(1);
            }

            Random rnd = new Random();
            String host = hosts[rnd.nextInt(hosts.length)];

            ExamplesUtil.runExample(client -> {
                try {
                    ApiServiceTransactionOptions transactionOptions =
                            new ApiServiceTransactionOptions(ETransactionType.TT_MASTER)
                                    .setSticky(true);

                    String node = "//tmp/test-node-cypress-example";
                    ApiServiceTransaction t = client.startTransaction(transactionOptions).get();


                    t.existsNode(node).thenAccept(result -> {
                        try {
                            if (result) {
                                t.removeNode(node);
                            }
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                    }).get();

                    YtGuid guid = t.createNode(node, ObjectType.Table).get();
                    /*
                    Map<String, YTreeNode> data = new HashMap<String, YTreeNode>();
                    data.put("k1", new YTreeInt64Node(10, new HashMap<>()));
                    data.put("k2", new YTreeInt64Node(31337, new HashMap<>()));
                    data.put("str", new YTreeStringNode("stroka"));
                    t.setNode(node, new YTreeMapNode(data)).get();
                    */
                    t.commit();

                    ApiServiceTransaction t2 = client.startTransaction(transactionOptions).get();
                    t2.linkNode(node, node + "-link");
                    t2.moveNode(node, node + "-moved");
                    t2.commit();

                    client.removeNode(node + "-link");

                } catch (Throwable e) {
                    System.out.println(e);
                    e.printStackTrace();
                    System.exit(-1);
                }
            }, ExamplesUtil.getUser(), ExamplesUtil.getToken(), host);

            System.exit(0);
        } catch (Throwable e) {
            System.out.println(e);
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
