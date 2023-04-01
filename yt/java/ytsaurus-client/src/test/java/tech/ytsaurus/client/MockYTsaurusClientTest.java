package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.Test;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.SelectRowsRequest;
import tech.ytsaurus.core.rows.YTreeMapNodeSerializer;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MockYTsaurusClientTest {
    String path = "//home/test-path";
    TableSchema schema = new TableSchema.Builder()
            .addKey("key", ColumnValueType.STRING)
            .build();

    @Test
    public void testOk() {
        MockYTsaurusClient mockClient = new MockYTsaurusClient("tmp");
        List<YTreeNode> firstExpectedResult = List.of(
                YTree.mapBuilder().key("key").value("1").buildMap(),
                YTree.mapBuilder().key("key").value("2").buildMap(),
                YTree.mapBuilder().key("key").value("3").buildMap());
        List<YTreeNode> secondExpectedResult = List.of(
                YTree.mapBuilder().key("key").value("4").buildMap(),
                YTree.mapBuilder().key("key").value("5").buildMap());

        assertEquals("times called value", 0, mockClient.getTimesCalled("lookupRows"));
        assertEquals("times called value", 0, mockClient.getTimesCalled("selectRows"));

        mockClient.mockMethod("lookupRows", () -> CompletableFuture.completedFuture(firstExpectedResult));
        mockClient.mockMethod("lookupRows", () -> CompletableFuture.completedFuture(secondExpectedResult));

        CompletableFuture<List<YTreeMapNode>> firstResult = doLookup(mockClient);
        assertEquals("expected mocked value", firstResult.join(), firstExpectedResult);
        assertEquals("times called value", 1, mockClient.getTimesCalled("lookupRows"));
        assertEquals("times called value", 0, mockClient.getTimesCalled("selectRows"));

        List<YTreeMapNode> thirdExpectedResult = List.of(
                YTree.mapBuilder().key("key").value("7").buildMap(),
                YTree.mapBuilder().key("key").value("8").buildMap(),
                YTree.mapBuilder().key("key").value("9").buildMap());
        mockClient.mockMethod("selectRows", () -> CompletableFuture.completedFuture(thirdExpectedResult));

        CompletableFuture<List<YTreeMapNode>> secondResult = doLookup(mockClient);
        assertEquals("expected mocked value", secondResult.join(), secondExpectedResult);
        assertEquals("times called value", 2, mockClient.getTimesCalled("lookupRows"));
        assertEquals("times called value", 0, mockClient.getTimesCalled("selectRows"));

        CompletableFuture<List<YTreeMapNode>> thirdResult = doSelect(mockClient);
        assertEquals("expected mocked value", thirdResult.join(), thirdExpectedResult);
        assertEquals("times called value", 2, mockClient.getTimesCalled("lookupRows"));
        assertEquals("times called value", 1, mockClient.getTimesCalled("selectRows"));

        mockClient.flushTimesCalled("selectRows");
        assertEquals("times called value", 2, mockClient.getTimesCalled("lookupRows"));
        assertEquals("times called value", 0, mockClient.getTimesCalled("selectRows"));

        mockClient.flushTimesCalled();
        assertEquals("times called value", 0, mockClient.getTimesCalled("lookupRows"));
        assertEquals("times called value", 0, mockClient.getTimesCalled("selectRows"));
    }

    @Test
    public void testNoMockedValue() {
        MockYTsaurusClient mockClient = new MockYTsaurusClient("tmp");

        List<YTreeMapNode> expectedResult = List.of(
                YTree.mapBuilder().key("key").value("1").buildMap(),
                YTree.mapBuilder().key("key").value("2").buildMap(),
                YTree.mapBuilder().key("key").value("3").buildMap());
        mockClient.mockMethod("lookupRows", () -> CompletableFuture.completedFuture(expectedResult));

        CompletableFuture<List<YTreeMapNode>> result = doLookup(mockClient);
        assertEquals("expected mocked value", result.join(), expectedResult);

        CompletableFuture<List<YTreeMapNode>> failedResult = doLookup(mockClient);
        assertTrue("no mocked value", failedResult.isCompletedExceptionally());
        try {
            failedResult.join();
            fail("no mocked value");
        } catch (CompletionException ex) {
            assertEquals("no mocked value message", "Method lookupRows wasn't mocked", ex.getCause().getMessage());
        }
    }

    private CompletableFuture<List<YTreeMapNode>> doLookup(MockYTsaurusClient mockClient) {
        return mockClient.lookupRows(
                new LookupRowsRequest(path, schema.toLookup()),
                new YTreeMapNodeSerializer());
    }

    private CompletableFuture<List<YTreeMapNode>> doSelect(MockYTsaurusClient mockClient) {
        return mockClient.selectRows(
                SelectRowsRequest.of("select something"),
                new YTreeMapNodeSerializer());
    }
}
