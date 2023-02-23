package tech.ytsaurus.client;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.bus.BusConnector;
import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.request.MultiTablePartition;
import tech.ytsaurus.client.request.PartitionTables;
import tech.ytsaurus.client.request.PartitionTablesMode;
import tech.ytsaurus.client.rpc.Compression;
import tech.ytsaurus.client.rpc.RpcCompression;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.RangeLimit;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

import ru.yandex.yt.ytclient.proxy.request.WriteTable;

public class PartitionTablesTest {
    YtClient yt;
    YPath tablePath;

    @Before
    public void init() {
        BusConnector connector = new DefaultBusConnector(new NioEventLoopGroup(0), true);
        String localProxy = System.getenv("YT_PROXY");

        yt = new YtClient(
                connector,
                List.of(new YtCluster(localProxy)),
                "default",
                null,
                YTsaurusClientAuth.builder()
                        .setUser("root")
                        .setToken("")
                        .build(),
                new RpcCompression().setRequestCodecId(Compression.None),
                new RpcOptions());

        yt.waitProxies().join();

        tablePath = YPath.simple("//tmp/partition-test-table");

        if (!yt.existsNode(tablePath.justPath().toString()).join()) {
            yt.createNode(tablePath.justPath().toString(), CypressNodeType.TABLE).join();
        }
        var schema = TableSchema.builder().add(new ColumnSchema("value", TiType.string())).build();

        TableWriter<YTreeMapNode> writer = yt.writeTable(new WriteTable<>(tablePath, YTreeMapNode.class)).join();
        try {
            writer.write(List.of(
                    YTree.mapBuilder().key("value").value("value_1").buildMap(),
                    YTree.mapBuilder().key("value").value("value_2").buildMap(),
                    YTree.mapBuilder().key("value").value("value_3").buildMap()
            ), schema);
            writer.close().join();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        writer = yt.writeTable(new WriteTable<>(tablePath.append(true), YTreeMapNode.class)).join();
        try {
            writer.write(List.of(
                    YTree.mapBuilder().key("value").value("value_4").buildMap(),
                    YTree.mapBuilder().key("value").value("value_5").buildMap(),
                    YTree.mapBuilder().key("value").value("value_6").buildMap()
            ), schema);
            writer.close().join();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testBasic() {
        var result = yt.partitionTables(
                new PartitionTables(List.of(tablePath), PartitionTablesMode.Unordered, DataSize.fromBytes(30))
        ).join();

        var resultPaths = result.stream().map(MultiTablePartition::getTableRanges).collect(Collectors.toList());

        Assert.assertEquals(List.of(
                List.of(tablePath.withRange(RangeLimit.row(0), RangeLimit.row(3))),
                List.of(tablePath.withRange(RangeLimit.row(3), RangeLimit.row(6)))
        ), resultPaths);
    }

    @Test(expected = CompletionException.class)
    public void testMaxPartitionCountExceeded() {
        var request = PartitionTables
                .builder()
                .setPaths(List.of(tablePath))
                .setPartitionMode(PartitionTablesMode.Unordered)
                .setDataWeightPerPartition(DataSize.fromBytes(1))
                .setAdjustDataWeightPerPartition(false)
                .setMaxPartitionCount(1)
                .build();

        var result = yt.partitionTables(request).join();
    }

    @Test
    public void testMaxPartitionCountWithDataWeightAdjustment() {
        var request = PartitionTables
                .builder()
                .setPaths(List.of(tablePath))
                .setPartitionMode(PartitionTablesMode.Unordered)
                .setDataWeightPerPartition(DataSize.fromBytes(1))
                .setMaxPartitionCount(1)
                .build();

        var result = yt.partitionTables(request).join();

        var resultPaths = result.stream().map(MultiTablePartition::getTableRanges).collect(Collectors.toList());

        Assert.assertEquals(List.of(
                List.of(tablePath.withRange(RangeLimit.row(0), RangeLimit.row(6)))
        ), resultPaths);
    }
}
