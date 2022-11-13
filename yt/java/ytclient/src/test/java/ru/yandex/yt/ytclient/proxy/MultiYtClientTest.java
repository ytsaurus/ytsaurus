package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;
import tech.ytsaurus.ysontree.YTree;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.rpcproxy.ETableReplicaMode;
import ru.yandex.yt.ytclient.request.TabletInfo;
import ru.yandex.yt.ytclient.request.TabletInfoReplica;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.MatcherAssert.assertThat;

public class MultiYtClientTest {
    String path = "//home/test-path";
    TableSchema schema = new TableSchema.Builder()
            .addKey("key", ColumnValueType.STRING)
            .build();

    @Test
    public void testInitialPenalty() throws Exception {
        MockYtClient hahnClient = new MockYtClient("hahn");
        MockYtClient freudClient = new MockYtClient("freud");

        hahnClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.completedFuture(List.of(Row.of("hahn value"))));

        freudClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.completedFuture(List.of(Row.of("freud value"))));

        MultiYtClient multiClient = MultiYtClient.builder()
                .addClients(
                        MultiYtClient.YtClientOptions.builder(freudClient).build(),
                        MultiYtClient.YtClientOptions.builder(hahnClient)
                                .setInitialPenalty(Duration.ofMillis(50)).build()
                )
                .build();

        var result = doLookup(multiClient);
        assertThat("Expected response from freud", result.join().get(0).key.equals("freud value"));
    }

    @Test
    public void testSameCluster() throws Exception {
        MockYtClient hahnClient = new MockYtClient("hahn");
        MockYtClient otherHahnClient = new MockYtClient("hahn");

        try {
            MultiYtClient.builder()
                    .addClients(
                            MultiYtClient.YtClientOptions.builder(hahnClient).build(),
                            MultiYtClient.YtClientOptions.builder(otherHahnClient).build()
                    )
                    .build();

            assertThat("Exception was expected", false);
        } catch (Exception ex) {
        }
    }

    @Test
    public void testAllFail() {
        MockYtClient hahnClient = new MockYtClient("hahn");
        MockYtClient freudClient = new MockYtClient("freud");

        hahnClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.failedFuture(new InternalError("Error")));

        freudClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.failedFuture(new InternalError("Error")));

        MultiYtClient multiClient = MultiYtClient.builder()
                .addClients(
                        MultiYtClient.YtClientOptions.builder(hahnClient).build(),
                        MultiYtClient.YtClientOptions.builder(freudClient).build()
                )
                .build();

        var result = doLookup(multiClient);
        try {
            result.join();
            assertThat("Exception was expected", false);
        } catch (Exception ex) {
        }
    }

    @Test
    public void testAllFailWithPenalty() {
        MockYtClient hahnClient = new MockYtClient("hahn");
        MockYtClient freudClient = new MockYtClient("freud");

        hahnClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.failedFuture(new InternalError("Error")));

        freudClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.failedFuture(new InternalError("Error")));

        MultiYtClient multiClient = MultiYtClient.builder()
                .addClients(
                        MultiYtClient.YtClientOptions.builder(hahnClient)
                                .setInitialPenalty(Duration.ofMillis(100))
                                .build(),
                        MultiYtClient.YtClientOptions.builder(freudClient).build()
                )
                .build();

        var result = doLookup(multiClient);
        try {
            result.join();
            assertThat("Exception was expected", false);
        } catch (Exception ex) {
        }
    }

    @Test
    public void testLagPenaltyProvider() throws InterruptedException {
        MockYtClient hahnClient = new MockYtClient("hahn");
        MockYtClient freudClient = new MockYtClient("freud");
        MockYtClient pythiaClient = new MockYtClient("pythia");

        hahnClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.completedFuture(List.of(Row.of("hahn value"))),
                3);

        freudClient.mockMethod(
                "lookupRows",
                () -> CompletableFuture.completedFuture(List.of(Row.of("freud value"))),
                3);

        GUID replicaIdHahn = GUID.valueOf("23f18-8a2d0-3f302c5-2188b2fc");
        GUID replicaIdFreud = GUID.valueOf("23f18-a0f8f-3f302c5-6ab4b47b");

        pythiaClient.mockMethod(
                "getNode",
                () -> CompletableFuture.completedFuture(
                        YTree.builder()
                                .beginAttributes()
                                .key("replicas")
                                .beginMap()
                                    .key(replicaIdHahn.toString())
                                    .beginMap()
                                        .key("mode").value("async")
                                        .key("cluster_name").value("hahn")
                                        .key("replica_path").value("//path-to-table")
                                        .key("state").value("enabled")
                                        .key("replication_lag_time").value(0)
                                        .key("replicated_table_tracker_enabled").value(true)
                                        .key("error_count").value(0)
                                    .endMap()
                                    .key(replicaIdFreud.toString())
                                    .beginMap()
                                        .key("mode").value("async")
                                        .key("cluster_name").value("freud")
                                        .key("replica_path").value("//path-to-table")
                                        .key("state").value("enabled")
                                        .key("replication_lag_time").value(0)
                                        .key("replicated_table_tracker_enabled").value(true)
                                        .key("error_count").value(0)
                                    .endMap()
                                .endMap()
                                .endAttributes()
                                .entity().build()
                )
        );

        pythiaClient.mockMethod(
                "getNode",
                () -> CompletableFuture.completedFuture(
                        YTree.builder().beginAttributes().key("tablet_count").value(5).endAttributes().entity().build())
        );

        YtTimestamp now = YtTimestamp.fromInstant(Instant.now());

        pythiaClient.mockMethod(
                "getTabletInfos",
                () -> CompletableFuture.completedFuture(
                        List.of(
                                new TabletInfo(1, 1, 1,
                                        List.of(
                                                new TabletInfoReplica(replicaIdHahn, now.getValue(), ETableReplicaMode.TRM_SYNC),
                                                new TabletInfoReplica(replicaIdFreud, 100, ETableReplicaMode.TRM_ASYNC) // very old ts
                                        )
                                )
                        ))
        );

        MultiYtClient multiClient = MultiYtClient.builder()
                .addClients(
                        MultiYtClient.YtClientOptions.builder(freudClient).build(),
                        MultiYtClient.YtClientOptions.builder(hahnClient)
                                .setInitialPenalty(Duration.ofSeconds(5))
                                .build()
                )
                .setPenaltyProvider(PenaltyProvider.lagPenaltyProviderBuilder()
                        .setClient(pythiaClient)
                        .setTablePath(YPath.simple("//replica-path"))
                        .setReplicaClusters(List.of("hahn", "freud"))
                        .setMaxTabletLag(Duration.ofSeconds(1000))
                        .setLagPenalty(Duration.ofSeconds(1000))
                        .setClearPenaltiesOnErrors(true)
                        .setCheckPeriod(Duration.ofSeconds(15))
                        .build())
                .build();

        Thread.sleep(Duration.ofSeconds(10).toMillis());

        var result = doLookup(multiClient);
        assertThat("Expected response from hahn", result.join().get(0).key.equals("hahn value"));

        // During this time we get error inside LagReplicationPenaltyProvider and clear penalties
        Thread.sleep(Duration.ofSeconds(10).toMillis());
        result = doLookup(multiClient);
        assertThat("Expected response from freud", result.join().get(0).key.equals("freud value"));
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    @Test
    public void testBanPenaltyAndDuration() throws Exception {
        MockYtClient hahnClient = new MockYtClient("hahn");
        MockYtClient freudClient = new MockYtClient("freud");

        MultiYtClient multiClient = MultiYtClient.builder()
                .addClients(
                        MultiYtClient.YtClientOptions.builder(hahnClient).build(),
                        MultiYtClient.YtClientOptions.builder(freudClient)
                                .setInitialPenalty(Duration.ofMillis(50)).build()
                )
                .setBanDuration(Duration.ofMillis(200))
                .setBanPenalty(Duration.ofMillis(100))
                .build();

        // Freud - ok. Hahn - error. Ban hahn on 50ms.
        {
            hahnClient.mockMethod(
                    "lookupRows",
                    () -> CompletableFuture.failedFuture(new InternalError("Error")));

            freudClient.mockMethod(
                    "lookupRows",
                    () -> CompletableFuture.completedFuture(List.of(Row.of("freud value"))));

            var result = doLookup(multiClient);
            assertThat("Expected response from freud", result.join().get(0).key.equals("freud value"));
        }

        // Freud - ok. Hahn - ok, but with penalty=100ms. Response from freud.
        for (int i = 0; i < 1; ++i) {
            {
                hahnClient.mockMethod(
                        "lookupRows",
                        () -> CompletableFuture.completedFuture(List.of(Row.of("hahn value"))));

                freudClient.mockMethod(
                        "lookupRows",
                        () -> CompletableFuture.completedFuture(List.of(Row.of("freud value"))));

                var result = doLookup(multiClient);
                assertThat("Expected response from freud", result.join().get(0).key.equals("freud value"));
            }
        }

        Thread.sleep(200);  // Waiting badUntil (banDuration == 200ms).

        // Freud - ok. Hahn - ok. Response from hahn, because badUntil was reached and initialPenalty of freud = 50ms.
        {
            hahnClient.mockMethod(
                    "lookupRows",
                    () -> CompletableFuture.completedFuture(List.of(Row.of("hahn value"))));

            freudClient.mockMethod(
                    "lookupRows",
                    () -> CompletableFuture.completedFuture(List.of(Row.of("freud value"))));

            var result = doLookup(multiClient);
            assertThat("Expected response from hahn", result.join().get(0).key.equals("hahn value"));
        }
    }

    private CompletableFuture<List<MockYtClientTest.Row>> doLookup(MultiYtClient client) {
        return client.lookupRows(
                new LookupRowsRequest(path, schema.toLookup()),
                (YTreeObjectSerializer<MockYtClientTest.Row>)
                        YTreeObjectSerializerFactory.forClass(MockYtClientTest.Row.class));
    }

    @YTreeObject
    static class Row {
        String key;

        Row(String key) {
            this.key = key;
        }
        static MockYtClientTest.Row of(String value) {
            return new MockYtClientTest.Row(value);
        }
    }
}
