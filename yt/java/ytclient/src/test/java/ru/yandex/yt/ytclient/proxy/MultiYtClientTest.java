package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
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
