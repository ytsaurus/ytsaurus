package ru.yandex.yt.client.proxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Charsets;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.lang3.StringUtils;
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

import ru.yandex.bolts.collection.Cf;
import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.RangeLimit;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.misc.thread.ThreadUtils;
import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.bus.DefaultBusConnector;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransaction;
import ru.yandex.yt.ytclient.proxy.ApiServiceTransactionOptions;
import ru.yandex.yt.ytclient.proxy.MappedModifyRowsRequest;
import ru.yandex.yt.ytclient.proxy.SelectRowsRequest;
import ru.yandex.yt.ytclient.proxy.TableReader;
import ru.yandex.yt.ytclient.proxy.TableWriter;
import ru.yandex.yt.ytclient.proxy.YtClient;
import ru.yandex.yt.ytclient.proxy.YtCluster;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.RemoveNode;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcCompression;
import ru.yandex.yt.ytclient.rpc.RpcCredentials;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.internal.Compression;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

@RunWith(Parameterized.class)
public class YtClientTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(YtClientTest.class);

    @Parameterized.Parameters(name = "{0}")
    public static Object[] parameters() {
        return new Object[][]{
                {new RpcCompression()},
                {new RpcCompression(Compression.Zlib_4)},
                {new RpcCompression(Compression.Lz4)},
                {new RpcCompression(Compression.Lz4HighCompression, Compression.Zlib_9)}};
    }

    public static String getProxy() {
        return Objects.requireNonNull(System.getenv().get("YT_PROXY"), "Env variable YT_PROXY is required");
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
        return StringUtils.removeEnd(getPathPrefix(), "/") + "/ytclient-junit/" + UUID.randomUUID().toString();
    }

    private static DefaultBusConnector bus;

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

    @Parameterized.Parameter
    public RpcCompression compression;

    private YtClient client;
    private String path;

    @Before
    public void init() {

        final String proxy = getProxy();
        final String username = getUsername();
        final String token = getToken();

        final YtCluster cluster = new YtCluster(proxy);
        client = new YtClient(bus,
                Collections.singletonList(cluster),
                cluster.getName(),
                null,
                new RpcCredentials(username, token),
                compression,
                new RpcOptions());

        client.waitProxies().join();
        path = getPath();
    }

    @After
    public void cleanup() {
        if (client != null) {
            try {
                deleteDirectory(client, path);
            } finally {
                client.close();
            }
        }
    }

    @Test(timeout = 10000)
    public void executeSomeOperations() {
        final String dir1 = path + "/dir1";
        final String table1 = dir1 + "/table1";

        createDirectory(client, dir1);
        createTable(client, table1);

        final String query = String.format("* from [%s]", table1);
        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        Assert.assertEquals(0, client.selectRows(query).join().getRows().size());

        final Collection<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        insertData(client, table1, objects, serializer);

        final List<UnversionedRow> rows = client.selectRows(query).join().getRows();
        final List<MappedObject> mappedRows = client.selectRows(SelectRowsRequest.of(query), serializer).join();

        Assert.assertEquals(objects, mappedRows);

        Assert.assertEquals(Arrays.asList(
                new UnversionedRow(Arrays.asList(
                        new UnversionedValue(0, ColumnValueType.INT64, false, 1L),
                        new UnversionedValue(1, ColumnValueType.STRING, false, "test1".getBytes(Charsets.UTF_8)))),
                new UnversionedRow(Arrays.asList(
                        new UnversionedValue(0, ColumnValueType.INT64, false, 2L),
                        new UnversionedValue(1, ColumnValueType.STRING, false, "test2".getBytes(Charsets.UTF_8))))
        ), rows);
    }

    @Test(timeout = 10000)
    public void readTable() throws Exception {
        final String dir1 = path + "/dir1";
        final String table2 = dir1 + "/table2";

        final String path = YPath.simple(table2).toString();

        readTableImpl(dir1, table2, path, new MappedObject(1, "test1"), new MappedObject(2, "test2"));
    }

    @Test(timeout = 10000)
    public void readTableWithRange() throws Exception {
        final String dir1 = path + "/dir1";
        final String table3 = dir1 + "/table3";

        final String path = YPath.simple(table3)
                .withColumns("k1", "v1")
                .withExact(new RangeLimit(
                        Cf.list(new YTreeBuilder().value(1).build()),
                        -1,
                        -1))
                .toString();

        readTableImpl(dir1, table3, path, new MappedObject(1, "test1"));
    }

    private void readTableImpl(String dir, String table, String path, MappedObject... expect) throws Exception {
        createDirectory(client, dir);
        createTable(client, table, false);

        final YTreeObjectSerializer<MappedObject> serializer =
                (YTreeObjectSerializer<MappedObject>) YTreeObjectSerializerFactory.forClass(MappedObject.class);

        final Collection<MappedObject> objects = Arrays.asList(
                new MappedObject(1, "test1"),
                new MappedObject(2, "test2"));

        insertData(client, table, objects, serializer, false);

        LOGGER.info("Reading table from {}", path);

        final List<MappedObject> actual = new ArrayList<>();
        final TableReader<MappedObject> reader = client.readTable(new ReadTable<>(path, serializer)).join();
        while (reader.canRead()) {
            final List<MappedObject> read = reader.read();
            if (read != null && !read.isEmpty()) { // Could be null
                actual.addAll(read);
            }
        }

        Assert.assertEquals(Arrays.asList(expect), actual);
    }

    public static void deleteDirectory(YtClient client, String path) {
        client.removeNode(new RemoveNode(path)).join();
    }

    public static void createDirectory(YtClient client, String dir) {
        client.createNode(new CreateNode(YPath.simple(dir), CypressNodeType.MAP, Collections.emptyMap())
                .setRecursive(true)
                .setIgnoreExisting(false)).join();

    }

    public static void createTable(YtClient client, String table) {
        createTable(client, table, true);
    }

    public static void createTable(YtClient client, String table, boolean dynamic) {
        final Map<String, YTreeNode> attrs = YTree.mapBuilder()
                .key("dynamic").value(YTree.booleanNode(dynamic))
                .key("schema").value(YTree.builder()
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

                        .endList()

                        .build())
                .buildMap().asMap();

        client.createNode(new CreateNode(YPath.simple(table), CypressNodeType.TABLE, attrs)
                .setRecursive(false)
                .setIgnoreExisting(false)).join();

        if (dynamic) {
            while (true) {
                try {
                    client.mountTable(table).join();
                    return; // ---
                } catch (RuntimeException e) {
                    LOGGER.info("Unable to mount table {}, will retry in a bit", table);
                    ThreadUtils.sleep(500, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    public static <T> void insertData(YtClient client, String table, Collection<T> objects,
                                      YTreeObjectSerializer<T> serializer) {
        insertData(client, table, objects, serializer, true);
    }

    public static <T> void insertData(YtClient client, String table, Collection<T> objects,
                                      YTreeObjectSerializer<T> serializer, boolean dynamic) {
        final MappedModifyRowsRequest<T> request = new MappedModifyRowsRequest<>(table, serializer);
        request.addUpdates(objects);

        final ApiServiceTransactionOptions options =
                new ApiServiceTransactionOptions(ETransactionType.TT_MASTER).setSticky(true);

        try (ApiServiceTransaction tx = client.startTransaction(options).join()) {
            if (dynamic) {
                tx.modifyRows(request).join();
            } else {
                final TableWriter<T> writer = tx.writeTable(new WriteTable<T>(table, serializer)).join();
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
