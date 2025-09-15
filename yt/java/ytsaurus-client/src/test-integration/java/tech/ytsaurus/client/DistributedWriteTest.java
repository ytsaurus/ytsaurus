package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.DistributedWriteCookie;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.StartDistributedWriteSession;
import tech.ytsaurus.client.request.WriteFragmentResult;
import tech.ytsaurus.client.request.WriteSerializationContext;
import tech.ytsaurus.client.request.WriteTableFragment;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import static tech.ytsaurus.core.tables.ColumnValueType.INT64;

public class DistributedWriteTest extends YTsaurusClientTestBase {

    private YTsaurusClient ytClient;
    private YPath testTablePath;
    private static final TableSchema TEST_TABLE_SCHEMA = TableSchema.builder().addValue("a", INT64).build();
    private static final int COOKIE_COUNT = 10;
    private static final int BATCH_SIZE = 10;
    private static final Comparator<YTreeMapNode> RESULT_COMPARATOR =
            Comparator.comparingLong(node -> node.getLong("a"));

    private SerializationContext<UnversionedRow> createSerializationContext() {
        return new WriteSerializationContext<>(new UnversionedRowSerializer(TEST_TABLE_SCHEMA));
    }

    @Before
    public void setUp() {
        var ytFixture = createYtFixture();
        this.ytClient = ytFixture.getYt();
        testTablePath = ytFixture.testDirectory.child("table1");
    }

    private void doDistributedWriteTest(int from, boolean append) {
        YPath writePath = append ? testTablePath.plusAdditionalAttribute("append", true) : testTablePath;

        var startDWreq = StartDistributedWriteSession.builder()
                .setPath(writePath)
                .setCookieCount(COOKIE_COUNT)
                .build();

        DistributedWriteHandle dwHandle = ytClient.startDistributedWriteSession(startDWreq).join();
        Assert.assertEquals(COOKIE_COUNT, dwHandle.getCookies().size());

        List<WriteFragmentResult> writeResults = new ArrayList<>(COOKIE_COUNT);
        for (DistributedWriteCookie cookie: dwHandle.getCookies()) {
            WriteTableFragment<UnversionedRow> wtfReq = WriteTableFragment.<UnversionedRow>builder()
                    .setCookie(cookie)
                    .setSerializationContext(createSerializationContext())
                    .build();
            AsyncFragmentWriter<UnversionedRow> fragmentWriter = ytClient.writeTableFragment(wtfReq).join();
            var data = LongStream.range(from, from + BATCH_SIZE).mapToObj(i ->
                            new UnversionedRow(List.of(new UnversionedValue(0, INT64, false, i))))
                    .collect(Collectors.toList());
            fragmentWriter.write(data).join();
            WriteFragmentResult result = fragmentWriter.finish().join();
            writeResults.add(result);
            from += 10;
        }

        dwHandle.finish(writeResults).join();
    }

    @Test
    public void testDistributedWriteAndAppend() {
        var createNodeReq = CreateNode.builder()
                .setPath(testTablePath)
                .setType(CypressNodeType.TABLE)
                .addAttribute("schema", TEST_TABLE_SCHEMA.toYTree())
                .build();
        ytClient.createNode(createNodeReq).join();

        // Writing to empty table
        doDistributedWriteTest(0, false);

        List<YTreeMapNode> result = readTable(ytClient, testTablePath);
        result.sort(RESULT_COMPARATOR);

        List<YTreeMapNode> expected = LongStream.range(0, COOKIE_COUNT * BATCH_SIZE)
                .mapToObj(i -> YTree.mapBuilder().key("a").value(i).buildMap())
                .collect(Collectors.toList());
        Assert.assertEquals(expected, result);

        // Appending to existing table
        doDistributedWriteTest(COOKIE_COUNT * BATCH_SIZE, true);

        List<YTreeMapNode> appendResult = readTable(ytClient, testTablePath);
        appendResult.sort(RESULT_COMPARATOR);
        List<YTreeMapNode> appendExpected = LongStream.range(0, COOKIE_COUNT * BATCH_SIZE * 2)
                .mapToObj(i -> YTree.mapBuilder().key("a").value(i).buildMap())
                .collect(Collectors.toList());

        Assert.assertEquals(appendExpected, appendResult);
    }
}
