package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateShuffleReader;
import tech.ytsaurus.client.request.CreateShuffleWriter;
import tech.ytsaurus.client.request.ShuffleHandle;
import tech.ytsaurus.client.request.StartShuffle;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class ShuffleServiceTest extends YTsaurusClientTestBase {

    private static final int NUM_PARTITIONS = 3;
    private static final int NUM_MAPPERS = 4;
    private static final int TOTAL_SHUFFLE_ROWS = 372_936;
    private static final int EXPECTED_PARTITION_SIZE = TOTAL_SHUFFLE_ROWS / NUM_PARTITIONS;
    private static final TableSchema SHUFFLE_SCHEMA = TableSchema.builder()
            .setStrict(true)
            .add(new ColumnSchema("partition", TiType.int64()))
            .add(new ColumnSchema("data", TiType.string()))
            .build();

    private YTsaurusClient ytClient;

    @Before
    public void setUp() {
        var ytFixture = createYtFixture();
        this.ytClient = ytFixture.getYt();
    }

    @Test
    public void testShuffleServiceInteraction() throws Exception {
        // Start transaction
        try (var transaction = ytClient.startTransaction(StartTransaction.master()).join()) {
            var txId = transaction.getId();

            // Create shuffle
            ShuffleHandle shuffleHandle = startShuffle(txId);

            // Write to shuffle
            for (int mapperId = 0; mapperId < NUM_MAPPERS; mapperId++) {
                CreateShuffleWriter shuffleWriterReq = CreateShuffleWriter.builder()
                        .setHandle(shuffleHandle)
                        .setPartitionColumn("pc")
                        .setWriterIndex(mapperId)
                        .build();
                AsyncWriter<UnversionedRow> shuffleDataWriter = ytClient.createShuffleWriter(shuffleWriterReq).join();
                shuffleDataWriter
                        .write(generateTestData(mapperId).collect(Collectors.toList()))
                        .thenCompose(unused -> shuffleDataWriter.finish()).join();
            }

            //Read from shuffle for all mappers
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                readAndCheckPartition(shuffleHandle, partition, 0, NUM_MAPPERS, EXPECTED_PARTITION_SIZE);
            }

            //Read from shuffle for specified mappers range
            readAndCheckPartition(shuffleHandle, 0, 0, NUM_MAPPERS / 2, EXPECTED_PARTITION_SIZE / 2);
            readAndCheckPartition(shuffleHandle, 0, NUM_MAPPERS / 2, NUM_MAPPERS, EXPECTED_PARTITION_SIZE / 2);
        }
    }

    @Test
    public void testRetryShuffleWrite() throws Exception {
        try (var transaction = ytClient.startTransaction(StartTransaction.master()).join()) {
            var txId = transaction.getId();

            //Create shuffle
            ShuffleHandle shuffleHandle = startShuffle(txId);

            //Write to shuffle for mapper id = 0 three times
            for (int i = 0; i < 3; i++) {
                CreateShuffleWriter shuffleWriterReq = CreateShuffleWriter.builder()
                        .setHandle(shuffleHandle)
                        .setPartitionColumn("pc")
                        .setWriterIndex(0)
                        .setOverwriteExistingWriterData(true)
                        .build();

                AsyncWriter<UnversionedRow> shuffleDataWriter = ytClient.createShuffleWriter(shuffleWriterReq).join();
                shuffleDataWriter
                        .write(generateTestData(0).collect(Collectors.toList()))
                        .thenCompose(unused -> shuffleDataWriter.finish()).join();
            }

            // Read for 0 partition and check
            readAndCheckPartition(shuffleHandle, 0, 0, 1, EXPECTED_PARTITION_SIZE / 4);
        }
    }

    @Test
    public void testPullBasedShuffleWithSchema() throws Exception {
        try (var transaction = ytClient.startTransaction(StartTransaction.master()).join()) {
            var txId = transaction.getId();

            ShuffleHandle shuffleHandle = startShuffle(txId, SHUFFLE_SCHEMA, false);

            writeAllMappers(shuffleHandle);

            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                readAndCheckPartition(shuffleHandle, partition, 0, NUM_MAPPERS, EXPECTED_PARTITION_SIZE);
            }
        }
    }

    @Test
    public void testPushBasedShuffleWithSchema() throws Exception {
        try (var transaction = ytClient.startTransaction(StartTransaction.master()).join()) {
            var txId = transaction.getId();

            ShuffleHandle shuffleHandle = startShuffle(txId, SHUFFLE_SCHEMA, true);

            writeAllMappers(shuffleHandle);

            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                readAndCheckPartition(shuffleHandle, partition, 0, NUM_MAPPERS, EXPECTED_PARTITION_SIZE);
            }
        }
    }

    @Test
    public void testPushBasedShuffleWithConfig() throws Exception {
        try (var transaction = ytClient.startTransaction(StartTransaction.master()).join()) {
            var txId = transaction.getId();

            // fake push config just for serialization check
            YTreeMapNode pushConfig = YTree.mapBuilder()
                    .key("max_partition_buffer_size").value(8L * 1024 * 1024)
                    .key("enable_data_compression").value(true)
                    .buildMap();

            ShuffleHandle shuffleHandle = startShuffle(txId, SHUFFLE_SCHEMA, true, pushConfig);

            writeAllMappers(shuffleHandle);

            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                readAndCheckPartition(shuffleHandle, partition, 0, NUM_MAPPERS, EXPECTED_PARTITION_SIZE);
            }
        }
    }

    private void writeAllMappers(ShuffleHandle shuffleHandle) {
        for (int mapperId = 0; mapperId < NUM_MAPPERS; mapperId++) {
            CreateShuffleWriter shuffleWriterReq = CreateShuffleWriter.builder()
                    .setHandle(shuffleHandle)
                    .setPartitionColumn("partition")
                    .setWriterIndex(mapperId)
                    .build();
            AsyncWriter<UnversionedRow> shuffleDataWriter = ytClient.createShuffleWriter(shuffleWriterReq).join();
            shuffleDataWriter
                    .write(generateTestData(mapperId).collect(Collectors.toList()))
                    .thenCompose(unused -> shuffleDataWriter.finish()).join();
        }
    }

    private ShuffleHandle startShuffle(GUID txId) {
        return startShuffle(txId, null, false);
    }

    private ShuffleHandle startShuffle(GUID txId, TableSchema schema, boolean usePushBasedShuffle) {
        return startShuffle(txId, schema, usePushBasedShuffle, null);
    }

    private ShuffleHandle startShuffle(
            GUID txId,
            TableSchema schema,
            boolean usePushBasedShuffle,
            YTreeMapNode pushConfig
    ) {
        StartShuffle startShuffleReq = StartShuffle.builder()
                .setAccount("intermediate")
                .setPartitionCount(NUM_PARTITIONS)
                .setParentTransactionId(txId)
                .setReplicationFactor(1)
                .setSchema(schema)
                .setUsePushBasedShuffle(usePushBasedShuffle)
                .setPushConfig(pushConfig)
                .build();
        return ytClient.startShuffle(startShuffleReq).join();
    }

    private Stream<UnversionedRow> generateTestData(int mapperId) {
        return IntStream.rangeClosed(1, TOTAL_SHUFFLE_ROWS)
                .filter(i -> i % NUM_MAPPERS == mapperId)
                .mapToObj(i -> new UnversionedRow(List.of(
                        new UnversionedValue(0, ColumnValueType.INT64, false, (long) (i % NUM_PARTITIONS)),
                        new UnversionedValue(1, ColumnValueType.STRING, false,
                                (mapperId + "_" + Integer.toBinaryString(i)).getBytes())
        )));
    }

    private void readAndCheckPartition(
            ShuffleHandle shuffleHandle,
            int partition,
            int startMapIndex,
            int endMapIndex,
            int expectedSize
            ) throws Exception {
        Set<Integer> expectedMappers = IntStream.range(startMapIndex, endMapIndex).boxed().collect(Collectors.toSet());

        CreateShuffleReader shuffleReaderReq = CreateShuffleReader.builder()
                .setHandle(shuffleHandle)
                .setPartitionIndex(partition)
                .setRange(new CreateShuffleReader.Range(startMapIndex, endMapIndex))
                .build();

        AsyncReader<UnversionedRow> reader = ytClient.createShuffleReader(shuffleReaderReq).join();
        List<UnversionedRow> result = new ArrayList<>();
        reader.acceptAllAsync(result::add, Executors.newSingleThreadExecutor()).join();
        reader.close();

        Assert.assertEquals(expectedSize, result.size());
        Assert.assertEquals(expectedSize, result.stream().distinct().count());
        Set<Integer> actualMappers = new HashSet<>();
        for (UnversionedRow row : result) {
            String value = new String(row.getValues().get(1).bytesValue());
            int separator = value.indexOf("_");
            int mapperId = Integer.parseInt(value.substring(0, separator));
            actualMappers.add(mapperId);
            int n = Integer.parseInt(value.substring(separator + 1), 2);
            Assert.assertEquals(partition, n % NUM_PARTITIONS);
        }
        Assert.assertEquals(expectedMappers, actualMappers);
    }
}
