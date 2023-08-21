package tech.ytsaurus.example;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ColumnFilter;
import tech.ytsaurus.client.request.GetNode;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeNode;


public class ExampleCypressOperations {
    private ExampleCypressOperations() {
    }

    public static void main(String[] args) {
        // *** Create the client ***

        // You need to set up cluster address in YT_PROXY environment variable.
        var clusterAddress = System.getenv("YT_PROXY");
        if (clusterAddress == null || clusterAddress.isEmpty()) {
            throw new IllegalArgumentException("Environment variable YT_PROXY is empty");
        }

        // The most convenient way to create a client is through builder
        // The only required parameter to specify is the cluster.
        //
        // The YT token will be picked up from `~/.yt/token`, and the username from the operating system.
        YTsaurusClient client = YTsaurusClient.builder()
                .setCluster(clusterAddress)
                .build();

        // It is necessary to call close() on the client to shut down properly
        // The most reliable and convenient way to do this is with the try-with-resources statement.
        try (client) {
            // *** Simple requests ***

            // Most of the client methods return CompletableFuture
            // The request will be sent in the background.
            CompletableFuture<YTreeNode> listResult = client.listNode("/");

            // You can use the join method to wait for the result of the request.
            // It is recommended to read the CompletableFuture documentation:
            // https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/CompletableFuture.html
            System.out.println("listResult: " + listResult.join());

            // *** Using optional parameters in a request ***

            // There is a full version of the method for all requests.
            // You can pass a request object in it, specifying advanced options there.
            CompletableFuture<YTreeNode> getResult = client.getNode(
                    GetNode.builder()
                            .setPath(YPath.simple("//home/dev/tutorial"))
                            .setAttributes(ColumnFilter.of("account", "row_count"))
                            .setTimeout(Duration.ofSeconds(10))
                            .build()
            );
            System.out.println("getResult: " + getResult.join());

            // *** Errors ***

            // Method calls will not throw exceptions
            // (maybe except when there is some kind of software error or misconfiguration).
            CompletableFuture<YTreeNode> badListResult = client.listNode("//some/directory/that/does/not/exist");

            try {
                // If YT returns an error or a network error occurs as a result of executing a request,
                // then the corresponding exception will be stored in CompletableFuture.
                badListResult.join();
            } catch (CompletionException ex) {
                System.out.println("ERROR: " + ex);
            }
        }
    }
}
