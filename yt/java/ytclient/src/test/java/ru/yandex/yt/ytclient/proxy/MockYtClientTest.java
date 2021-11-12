package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.MatcherAssert.assertThat;

public class MockYtClientTest {
    String path = "//home/test-path";
    TableSchema schema = new TableSchema.Builder()
            .addKey("key", ColumnValueType.STRING)
            .build();

    @Test
    public void testOk() {
        MockYtClient mockClient = new MockYtClient("tmp");
        List<Row> firstExpectedResult = List.of(Row.of("1"), Row.of("2"), Row.of("3"));
        List<Row> secondExpectedResult = List.of(Row.of("4"), Row.of("5"));

        assertThat("times called value", mockClient.getTimesCalled("lookupRows") == 0);
        assertThat("times called value", mockClient.getTimesCalled("selectRows") == 0);

        mockClient.mockMethod("lookupRows", () -> CompletableFuture.completedFuture(firstExpectedResult));
        mockClient.mockMethod("lookupRows", () -> CompletableFuture.completedFuture(secondExpectedResult));

        CompletableFuture<List<Row>> firstResult = doLookup(mockClient);
        assertThat("expected mocked value", firstResult.join().equals(firstExpectedResult));
        assertThat("times called value", mockClient.getTimesCalled("lookupRows") == 1);
        assertThat("times called value", mockClient.getTimesCalled("selectRows") == 0);

        List<Row> thirdExpectedResult = List.of(Row.of("7"), Row.of("8"), Row.of("9"));
        mockClient.mockMethod("selectRows", () -> CompletableFuture.completedFuture(thirdExpectedResult));

        CompletableFuture<List<Row>> secondResult = doLookup(mockClient);
        assertThat("expected mocked value", secondResult.join().equals(secondExpectedResult));
        assertThat("times called value", mockClient.getTimesCalled("lookupRows") == 2);
        assertThat("times called value", mockClient.getTimesCalled("selectRows") == 0);

        CompletableFuture<List<Row>> thirdResult = doSelect(mockClient);
        assertThat("expected mocked value", thirdResult.join().equals(thirdExpectedResult));
        assertThat("times called value", mockClient.getTimesCalled("lookupRows") == 2);
        assertThat("times called value", mockClient.getTimesCalled("selectRows") == 1);

        mockClient.flushTimesCalled("selectRows");
        assertThat("times called value", mockClient.getTimesCalled("lookupRows") == 2);
        assertThat("times called value", mockClient.getTimesCalled("selectRows") == 0);

        mockClient.flushTimesCalled();
        assertThat("times called value", mockClient.getTimesCalled("lookupRows") == 0);
        assertThat("times called value", mockClient.getTimesCalled("selectRows") == 0);
    }

    @Test
    public void testNoMockedValue() {
        MockYtClient mockClient = new MockYtClient("tmp");

        List<Row> expectedResult = List.of(Row.of("1"), Row.of("2"), Row.of("3"));
        mockClient.mockMethod("lookupRows", () -> CompletableFuture.completedFuture(expectedResult));

        CompletableFuture<List<Row>> result = doLookup(mockClient);
        assertThat("expected mocked value", result.join().equals(expectedResult));

        CompletableFuture<List<Row>> failedResult = doLookup(mockClient);
        assertThat("no mocked value", failedResult.isCompletedExceptionally());
        try {
            failedResult.join();
            assertThat("no mocked value", false);
        } catch (CompletionException ex) {
            assertThat("no mocked value message",
                    ex.getCause().getMessage().equals("Method lookupRows wasn't mocked"));
        }
    }

    private CompletableFuture<List<Row>> doLookup(MockYtClient mockClient) {
        return mockClient.lookupRows(
                new LookupRowsRequest(path, schema.toLookup()),
                (YTreeObjectSerializer<Row>) YTreeObjectSerializerFactory.forClass(Row.class));
    }

    private CompletableFuture<List<Row>> doSelect(MockYtClient mockClient) {
        return mockClient.selectRows(
                SelectRowsRequest.of("select something"),
                (YTreeObjectSerializer<Row>) YTreeObjectSerializerFactory.forClass(Row.class));
    }

    @YTreeObject
    static class Row {
        String key;

        Row(String key) {
            this.key = key;
        }

        static Row of(String value) {
            return new Row(value);
        }
    }
}
