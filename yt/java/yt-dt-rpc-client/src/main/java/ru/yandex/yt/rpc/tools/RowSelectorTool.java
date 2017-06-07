package ru.yandex.yt.rpc.tools;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import ru.yandex.yt.rpc.client.YtDtAsyncClient;

/**
 * Created by aozeritsky on 14.04.2017.
 */
public class RowSelectorTool {
    public static void main(String [] args) {
        try {
            String hostname = args[0];
            int port = Integer.parseInt(args[1]);
            String query = args[2];
            String token = args[3];
            String user = args[4];
            String domain = args[5];

            int protocolVersion = 1; // << ??

            YtDtAsyncClient client = new YtDtAsyncClient(new InetSocketAddress(hostname, port), token, user, domain, protocolVersion);

            UUID requestId = UUID.randomUUID();

            CompletableFuture<List<Map<String, Object>>> future = client.selectRows(query, requestId, false, false);
            List<Map<String, Object>> res = future.join();
            int i = 0;
            for (Map<String, Object> row : res) {
                System.out.println(String.format("row %d:", i++));
                System.out.println(String.format("  %s", row));
            }
            System.exit(0);
        } catch (Throwable e) {
            System.out.println(String.format("error -> %s", e.toString()));
            System.exit(1);
        }
    }
}
