package tech.ytsaurus.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.request.AlterTable;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowDeserializer;
import tech.ytsaurus.client.rows.UnversionedRowSerializer;
import tech.ytsaurus.client.rows.UnversionedValue;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.RangeLimit;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.rpcproxy.ETransactionType;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.testlib.LocalYt;
import ru.yandex.yt.ytclient.proxy.MappedLookupRowsRequest;
import ru.yandex.yt.ytclient.proxy.MappedModifyRowsRequest;
import ru.yandex.yt.ytclient.proxy.YandexSerializationResolver;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.MountTable;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;

@RunWith(Parameterized.class)
public class YtClientTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YtClientTest.class);
    private static DefaultBusConnector bus;
    private YtClient client;
    private String path;
    @Parameterized.Parameter
    public RpcCompression compression;

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[][]{
                {new RpcCompression()},
                {new RpcCompression(Compression.Zlib_4)},
                {new RpcCompression(Compression.Lz4)},
                {new RpcCompression(Compression.Lz4HighCompression, Compression.Zlib_9)}
        };
    }

    public static String getUsername() {
        return Objects.requireNonNull(System.getenv().getOrDefault("YT_USERNAME", "root"));
    }

    public static String getToken() {
        return Objects.requireNonNull(System.getenv().getOrDefault("YT_TOKEN", ""));
    }

    public static String getPathPrefix() {
        return Objects.requireNonNull(System.getenv().getOrDefault("YT_PATH",
                "//home/" + System.getProperty("user.name")));
    }

    public static String getPath() {
        String prefix = getPathPrefix();
        if (prefix.endsWith("/")) {
            prefix = prefix.substring(0, prefix.length() - 1);
        }
        return prefix + "/ytclient-junit/" + UUID.randomUUID();
    }

    @BeforeClass
    public static void initBus() {
        bus = new DefaultBusConnector(new NioEventLoopGroup(2), true);
    }

    @AfterClass
    public static void closeBus() {
        if (bus != null) {
            bus.close();
        }
    }

    @Before
    public void init() {

        final String proxy = LocalYt.getAddress();
        final String username = getUsername();
        final String token = getToken();

        final YtCluster cluster = new YtCluster(proxy);
        client = new YtClient(bus,
                Collections.singletonList(cluster),
                cluster.getName(),
                null,
                YTsaurusClientAuth.builder()
                        .setUser(username)
                        .setToken(token)
                        .build(),
                compression,
                new RpcOptions().setUseClientsCache(true));

        client.waitProxies().join();
        path = getPath();
    }

    @After
    public void cleanup() {
        if (client != null) {
            try {
                deleteDirectory(client, YPath.simple(path));
            } finally {
                client.close();
            }
        }
    }

    @Test
    public void executeSomeOperations() {
        final YPath table = YPath.simple(path + "/dir1/table1");

        createDynamicTable(client, table);

        final String query = String.format("* from [%s]", table);
        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        Assert.assertEquals(0, client.selectRows(query).join().getRows().size());

        final Collection<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1", 100),
                new MappedObject(2, "test2", 200));

        insertData(client, table, objects, serializer);

        final List<UnversionedRow> rows = client.selectRows(query).join().getRows();
        final List<MappedObject> mappedRows = client.selectRows(SelectRowsRequest.of(query), serializer).join();

        Assert.assertEquals(objects, mappedRows);

        Assert.assertEquals(Arrays.asList(
                new UnversionedRow(Arrays.asList(
                        new UnversionedValue(0, ColumnValueType.INT64, false, 1L),
                        new UnversionedValue(1, ColumnValueType.STRING, false,
                                "test1".getBytes(StandardCharsets.UTF_8)),
                        new UnversionedValue(2, ColumnValueType.INT64, false, 100L))),
                new UnversionedRow(Arrays.asList(
                        new UnversionedValue(0, ColumnValueType.INT64, false, 2L),
                        new UnversionedValue(1, ColumnValueType.STRING, false,
                                "test2".getBytes(StandardCharsets.UTF_8)),
                        new UnversionedValue(2, ColumnValueType.INT64, false, 200L)))
        ), rows);
    }

    @Test
    public void readTable() throws Exception {
        final YPath writePath = YPath.simple(path + "/dir1/table2");

        final YPath readPath = writePath;

        readWriteImpl(writePath, readPath, new MappedObject(1, "test1"), new MappedObject(2, "test2"));
    }

    @Test
    public void readTableWithRange() throws Exception {
        final YPath writePath = YPath.simple(path + "/dir1/table3");

        final YPath readPath = YPath.simple(writePath.toString())
                .withColumns("k1", "v1")
                .withExact(RangeLimit.key(new YTreeBuilder().value(1).build()));

        readWriteImpl(writePath, readPath, new MappedObject(1, "test1"));
    }

    @Test
    public void alterTable() throws Exception {
        final YPath writePath = YPath.simple(path + "/dir1/table4");
        final YPath readPath = YPath.simple(writePath.toString());

        // Вставляем данные
        readWriteImpl(writePath, readPath, new MappedObject(1, "test1"), new MappedObject(2, "test2"));

        // Такая же схема - ничего не изменилось
        client.alterTable(AlterTable.builder().setPath(writePath).setSchema(schema()).build()).join();

        // Модифицируем - новый столбец
        client.alterTable(AlterTable.builder().setPath(writePath).setSchema(schema(b ->
                        b.beginMap()
                                .key("name").value("v2")
                                .key("type").value("string")
                                .endMap())).build())
                .join();
    }

    @Test
    public void selectRowsWithKnownPool() {
        final YPath table = YPath.simple(path + "/dir1/table5");

        createDynamicTable(client, table);

        final String poolName = "known_test_pool";
        client.createNode(new CreateNode(YPath.simple("//sys/ql_pools/" + poolName),
                CypressNodeType.MAP, Collections.singletonMap("weight", YTree.integerNode(5)))
                .setRecursive(true)
                .setIgnoreExisting(true));

        final String query = String.format("* from [%s]", table);

        final SelectRowsRequest request = SelectRowsRequest.of(query).setExecutionPool(poolName);
        Assert.assertEquals(0, client.selectRows(request).join().getRows().size());
    }

    @Test
    public void selectRowsV2WithKnownPool() {
        final YPath table = YPath.simple(path + "/dir1/table5");

        createDynamicTable(client, table);

        final String poolName = "known_test_pool";
        client.createNode(new CreateNode(YPath.simple("//sys/ql_pools/" + poolName),
                CypressNodeType.MAP, Collections.singletonMap("weight", YTree.integerNode(5)))
                .setRecursive(true)
                .setIgnoreExisting(true));

        final String query = String.format("* from [%s]", table);

        final SelectRowsRequest request = SelectRowsRequest.of(query).setExecutionPool(poolName);
        SelectRowsResult result = client.selectRowsV2(request).join();
        Assert.assertEquals(0, result.getUnversionedRowset().join().getRows().size());
        Assert.assertEquals(result.isIncompleteOutput(), false);
    }

    @Test
    public void lookupRowsDefault() throws ExecutionException, InterruptedException {
        final YPath table = YPath.simple(path + "/dir1/table6");
        createDynamicTable(client, table);

        final List<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        insertData(client, table, objects, serializer);

        final TableSchema schema = new TableSchema.Builder()
                .addKey("k1", ColumnValueType.INT64)
                .addValue("v1", ColumnValueType.STRING)
                .build()
                .toLookup();

        LookupRowsRequest request = new LookupRowsRequest(table.toString(), schema)
                .addLookupColumns("k1", "v1")
                .addFilter(2);
        List<UnversionedRow> result = client.lookupRows(request).get().getRows();

        Assert.assertEquals(1, result.size());
        Assert.assertEquals(new UnversionedRow(Arrays.asList(
                        new UnversionedValue(0, ColumnValueType.INT64, false, 2L),
                        new UnversionedValue(1, ColumnValueType.STRING, false,
                                "test2".getBytes(StandardCharsets.UTF_8)))),
                result.get(0));

    }

    @Test
    public void lookupRowsMapped() throws ExecutionException, InterruptedException {
        final YPath table = YPath.simple(path + "/dir1/table7");
        createDynamicTable(client, table);

        final List<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        insertData(client, table, objects, serializer);

        // Достаем только ключи
        MappedLookupRowsRequest<MappedObject> request1 = new MappedLookupRowsRequest<>(table.toString(), serializer)
                .addKeyLookupColumns()
                .addFilter(objects.get(1));
        List<MappedObject> result1 = client.lookupRows(request1, serializer).get();

        Assert.assertEquals(1, result1.size());
        Assert.assertEquals(new MappedObject(2, null), result1.get(0));
        Assert.assertNotSame(objects.get(0), result1.get(0));


        // Достаем все поля
        MappedLookupRowsRequest<MappedObject> request2 = new MappedLookupRowsRequest<>(table.toString(), serializer)
                .addAllLookupColumns()
                .addFilter(objects.get(1));
        List<MappedObject> result2 = client.lookupRows(request2, serializer).get();

        Assert.assertEquals(1, result2.size());
        Assert.assertEquals(objects.get(1), result2.get(0));
        Assert.assertNotSame(objects.get(1), result2.get(0));

        // Достаем все поля и все записи
        MappedLookupRowsRequest<MappedObject> request3 = new MappedLookupRowsRequest<>(table.toString(), serializer)
                .addAllLookupColumns()
                .addFilters(objects);
        List<MappedObject> result3 = client.lookupRows(request3, serializer).get();

        Assert.assertEquals(2, result3.size());
        Assert.assertEquals(objects, result3);

        // Достаем все поля и все записи, передавая списка ключей в обратном порядке
        // Результатом будут записи также в обратном (указанном) порядке
        MappedLookupRowsRequest<MappedObject> request4 = new MappedLookupRowsRequest<>(table.toString(), serializer)
                .addAllLookupColumns()
                .addFilter(new MappedObject(2, null))
                .addFilter(new MappedObject(1, null));
        List<MappedObject> result4 = client.lookupRows(request4, serializer).get();

        Assert.assertEquals(2, result4.size());
        Assert.assertEquals(List.of(objects.get(1), objects.get(0)), result4);
    }

    @Test
    public void earlyReaderClose() throws IOException {
        String table = path + "/table";

        client.createNode(new CreateNode(table, CypressNodeType.TABLE).setRecursive(true)).join();

        {
            TableWriter<MappedObject> writer =
                    client.writeTable(
                                    new WriteTable<>(table, YTreeObjectSerializerFactory.forClass(MappedObject.class)))
                            .join();
            List<MappedObject> data = new ArrayList<>();
            for (int i = 0; i < 1000; ++i) {
                data.add(new MappedObject(i, Integer.toString(i)));
            }
            writer.write(data);
            writer.readyEvent().join();
            writer.close().join();
        }

        {
            TableReader<MappedObject> reader = client.readTable(
                            new ReadTable<>(
                                    table,
                                    YTreeObjectSerializerFactory.forClass(MappedObject.class)))
                    .join();

            reader.close().join();
        }
    }

    private byte[] codeList(List<Long> list) {
        YTreeBuilder builder = new YTreeBuilder();
        builder.onBeginList();
        for (int i = 0; i < list.size(); i++) {
            UnversionedValue key = new UnversionedValue(
                    i,
                    ColumnValueType.INT64,
                    false,
                    list.get(i));
            builder.onListItem();
            key.writeTo(builder);
        }
        builder.onEndList();
        return builder.build().toBinary();
    }

    @Test
    public void writeUnversionedRow() throws Exception {
        String table = path + "/table";
        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("array", TiType.list(TiType.int64()))
                .build();

        client.createNode(new CreateNode(table, CypressNodeType.TABLE, Map.of(
                "schema", schema.toYTree()
        )).setRecursive(true)).join();

        List<UnversionedRow> data =
                Stream.of(List.of(0L, 1L), List.of(2L))
                        .map(row -> new UnversionedRow(List.of(new UnversionedValue(
                                0,
                                ColumnValueType.COMPOSITE,
                                false,
                                codeList(row)))))
                        .collect(Collectors.toList());

        {
            TableWriter<UnversionedRow> writer =
                    client.writeTable(
                            new WriteTable<>(table, new UnversionedRowSerializer(schema))
                    ).join();

            while (!writer.write(data, schema)) {
                writer.readyEvent().join();
            }
            writer.close().join();
        }

        {
            TableReader<UnversionedRow> reader =
                    client.readTable(new ReadTable<>(table, new UnversionedRowDeserializer())).join();
            List<UnversionedRow> result;
            while ((result = reader.read()) == null) {
                reader.readyEvent().join();
            }

            Assert.assertEquals(data, result);
        }
    }

    @Test
    public void testInsertWithFieldFilter() {
        YPath table = YPath.simple(path + "/table8");
        createDynamicTable(client, table);

        final String query = String.format("* from [%s]", table);

        final YTreeObjectSerializer<MappedObject> limitedSerializer =
                new YTreeObjectSerializer<>(MappedObject.class, field -> !field.getName().equals("v1"));

        insertData(client, table, Arrays.asList(
                new MappedObject(1, "test1", 100),
                new MappedObject(2, "test2", 200)), limitedSerializer);

        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        List<MappedObject> expect = Arrays.asList(
                new MappedObject(1, "", 100),
                new MappedObject(2, "", 200));
        Assert.assertEquals(expect, client.selectRows(SelectRowsRequest.of(query), serializer).join());
    }

    @Test
    public void testSelectWithFieldFilter() {
        YPath table = YPath.simple(path + "/table8");
        createDynamicTable(client, table);

        final String query = String.format("* from [%s]", table);

        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        insertData(client, table, Arrays.asList(
                new MappedObject(1, "test1", 100),
                new MappedObject(2, "test2", 200)), serializer);


        final YTreeObjectSerializer<MappedObject> limitedSerializer =
                new YTreeObjectSerializer<>(MappedObject.class, field -> !field.getName().equals("v1"));

        // Поле полностью игнорируется десериализатором - оно не будет заполнено пустой строкой
        List<MappedObject> expect = Arrays.asList(
                new MappedObject(1, null, 100),
                new MappedObject(2, null, 200));
        Assert.assertEquals(expect, client.selectRows(SelectRowsRequest.of(query), limitedSerializer).join());
    }

    @Test
    public void testPreparedModifyRowsRequest() {
        YPath table = YPath.simple(path + "/table9");

        var schema = TableSchema.builder()
                .setUniqueKeys(true)
                .addKey("key", TiType.string())
                .addValue("value", TiType.string())
                .build();

        client.createNode(new CreateNode(table, CypressNodeType.TABLE)
                .setRecursive(true)
                .addAttribute("dynamic", true)
                .addAttribute("schema", schema.toYTree())
        ).join();

        client.mountTableAndWaitTablets(new MountTable(table)).join();

        var tx = client.startTransaction(StartTransaction.tablet()).join();
        try (tx) {
            tx.modifyRows(ModifyRowsRequest.builder().setPath(table.toString()).setSchema(schema)
                    .addInsert(Map.of("key", "foo", "value", "bar"))
                    .build()
                    .prepare(compression, YandexSerializationResolver.getInstance())
            ).join();
            tx.commit().join();
        }

        var actualRows = client.lookupRows(
                new LookupRowsRequest(table.toString(), schema.toLookup())
                        .addFilter("foo")
        ).join().getRows();

        Assert.assertEquals(
                actualRows,
                List.of(
                        new UnversionedRow(Arrays.asList(
                                new UnversionedValue(0, ColumnValueType.STRING, false, "foo".getBytes()),
                                new UnversionedValue(1, ColumnValueType.STRING, false, "bar".getBytes())
                        ))));
    }

    private void readWriteImpl(YPath writePath, YPath readPath, MappedObject... expect) throws Exception {
        createStaticTable(client, writePath);

        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        final Collection<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        LOGGER.info("Inserting: {}", objects);

        insertData(client, writePath, objects, serializer, false);

        LOGGER.info("Reading table from {}", readPath);

        List<MappedObject> actual = readData(client, readPath, serializer);

        Assert.assertEquals(Arrays.asList(expect), actual);
    }


    public static void deleteDirectory(YtClient client, YPath path) {
        client.removeNode(new RemoveNode(path)).join();
    }

    public static void createStaticTable(YtClient client, YPath table) {
        createTable(client, table, false);
    }

    public static void createDynamicTable(YtClient client, YPath table) {
        createTable(client, table, true);
    }

    static YTreeNode schema() {
        return schema(b -> {
        });
    }

    static YTreeNode schema(Consumer<YTreeBuilder> additionalColumns) {
        final YTreeBuilder builder = YTree.builder()
                .beginAttributes()
                .key("unique_keys").value(true)
                .key("strict").value(true)
                .endAttributes()

                .beginList()

                .beginMap()
                .key("name").value("k1")
                .key("required").value(true)
                .key("type").value("int32")
                .key("sort_order").value("ascending")
                .endMap()

                .beginMap()
                .key("name").value("v1")
                .key("type").value("string")
                .endMap()

                .beginMap()
                .key("name").value("l1")
                .key("type").value("int64")
                .endMap();

        additionalColumns.accept(builder);

        return builder
                .endList()
                .build();
    }

    public static void createTable(YtClient client, YPath table, boolean dynamic) {
        createTable(client, table, schema(), dynamic);
    }

    public static void createTable(YtClient client, YPath table, YTreeNode schema, boolean dynamic) {
        LOGGER.info("Creating {} table: {}", dynamic ? "dynamic" : "static", table);

        final Map<String, YTreeNode> attrs = YTree.mapBuilder()
                .key("dynamic").value(YTree.booleanNode(dynamic))
                .key("schema").value(schema)
                .buildMap().asMap();

        client.createNode(new CreateNode(table, CypressNodeType.TABLE, attrs)
                .setRecursive(true)
                .setIgnoreExisting(false)).join();

        if (dynamic) {
            LOGGER.info("Waiting for table mount: {}", table);
            client.mountTable(table.toString(), null, false, true).join();
        }
    }

    public static <T> List<T> readData(YtClient client, YPath path,
                                       YTreeObjectSerializer<T> serializer) throws Exception {
        LOGGER.info("Reading from {}", path.toString());
        final List<T> actual = new ArrayList<>();
        final TableReader<T> reader = client.readTable(new ReadTable<>(path, serializer)).join();
        try {
            while (reader.canRead()) {
                while (true) {
                    final List<T> read = reader.read();
                    if (read != null && !read.isEmpty()) { // Could be null
                        actual.addAll(read);
                    } else {
                        break;
                    }
                }
                reader.readyEvent().join();
            }
        } finally {
            reader.close().join();
        }
        return actual;
    }

    public static <T> void insertData(YtClient client, YPath table, Collection<T> objects,
                                      YTreeObjectSerializer<T> serializer) {
        insertData(client, table, objects, serializer, true);
    }

    public static <T> void insertData(YtClient client, YPath table, Collection<T> objects,
                                      YTreeObjectSerializer<T> serializer, boolean dynamic) {
        LOGGER.info("Inserting {} rows into {} table: {}", objects.size(), dynamic ? "dynamic" : "static", table);

        final ApiServiceTransactionOptions options =
                new ApiServiceTransactionOptions(ETransactionType.TT_MASTER).setSticky(true);

        try (ApiServiceTransaction tx = client.startTransaction(options).join()) {
            if (dynamic) {
                final MappedModifyRowsRequest<T> request = new MappedModifyRowsRequest<>(table.toString(), serializer);
                request.addInserts(objects);
                tx.modifyRows(request).join();
            } else {
                final TableWriter<T> writer = tx.writeTable(new WriteTable<>(table, serializer)).join();
                writer.write(new ArrayList<>(objects));
                writer.readyEvent().join();
                writer.close().join();
            }
            tx.commit().join();
        } catch (IOException e) {
            throw new RuntimeException("Unable to write", e);
        }
    }

}
