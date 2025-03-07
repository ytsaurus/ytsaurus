package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateShuffleReader;
import tech.ytsaurus.client.request.CreateShuffleWriter;
import tech.ytsaurus.client.request.ShuffleHandle;
import tech.ytsaurus.client.request.StartShuffle;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.ysontree.YTreeNode;

public class ShuffleServiceTest extends YTsaurusClientTestBase {

    @Test
    public void testShuffleServiceInteraction() throws Exception {
        var ytFixture = createYtFixture();
        var ytClient = ytFixture.yt;
        final int numPartitions = 3;
        final int totalShuffleRows = 93_234;

        // Start transaction
        try (var transaction = ytClient.startTransaction(StartTransaction.master()).join()) {
            var txId = transaction.getId();

            // Create shuffle
            StartShuffle startShuffleReq = StartShuffle.builder()
                    .setAccount("intermediate")
                    .setPartitionCount(numPartitions)
                    .setParentTransactionId(txId)
                    .setReplicationFactor(1)
                    .build();
            ShuffleHandle shuffleHandle = ytClient.startShuffle(startShuffleReq).join();
            Map<String, YTreeNode> handleYson = shuffleHandle.asYTreeNode().asMap();
            Assert.assertEquals(numPartitions, handleYson.get("partition_count").intValue());
            Assert.assertEquals(1, handleYson.get("replication_factor").intValue());
            Assert.assertEquals("intermediate", handleYson.get("account").stringValue());

            // Write to shuffle
            CreateShuffleWriter shuffleWriterReq = CreateShuffleWriter.builder()
                    .setHandle(shuffleHandle)
                    .setPartitionColumn("pc")
                    .build();
            AsyncWriter<UnversionedRow> shuffleDataWriter = ytClient.createShuffleWriter(shuffleWriterReq).join();
            shuffleDataWriter
                    .write(generateTestData(numPartitions, totalShuffleRows).collect(Collectors.toList()))
                    .thenCompose(unused -> shuffleDataWriter.finish()).join();

            //Read from shuffle
            for (int partition = 0; partition < numPartitions; partition++) {
                CreateShuffleReader shuffleReaderReq = CreateShuffleReader.builder()
                        .setHandle(shuffleHandle)
                        .setPartitionIndex(partition)
                        .build();

                AsyncReader<UnversionedRow> reader = ytClient.createShuffleReader(shuffleReaderReq).join();
                ArrayList<UnversionedRow> result = new ArrayList<>();
                reader.acceptAllAsync(result::add, Executors.newSingleThreadExecutor()).join();
                reader.close();

                Assert.assertEquals(totalShuffleRows / numPartitions, result.size());
                Assert.assertEquals(totalShuffleRows / numPartitions, result.stream().distinct().count());
                final int expectedPartition = partition;
                boolean allRowsMatch = result.stream().allMatch(row -> {
                    int n = Integer.parseInt(new String(row.getValues().get(1).bytesValue()), 2);
                    return n % numPartitions == expectedPartition;
                });
                Assert.assertTrue(allRowsMatch);
            }
        }
    }

    private Stream<UnversionedRow> generateTestData(int numPartitions, int total) {
        return IntStream.rangeClosed(1, total).mapToObj(i -> new UnversionedRow(List.of(
                new UnversionedValue(0, ColumnValueType.INT64, false, (long) (i % numPartitions)),
                new UnversionedValue(1, ColumnValueType.STRING, false, Integer.toBinaryString(i).getBytes())
        )));
    }
}
