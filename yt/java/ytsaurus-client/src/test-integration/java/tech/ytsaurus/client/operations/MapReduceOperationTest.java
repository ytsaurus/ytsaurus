package tech.ytsaurus.client.operations;

import java.util.Iterator;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.YTsaurusClientTestBase;
import tech.ytsaurus.client.request.MapReduceOperation;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.testlib.proto.MapInputProtoType;
import tech.ytsaurus.testlib.proto.MapOutputProtoType;
import tech.ytsaurus.testlib.proto.ReduceOutputProtoType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;


public class MapReduceOperationTest extends YTsaurusClientTestBase {
    public static class SimpleMapperYTreeMapNode implements Mapper<YTreeMapNode, YTreeMapNode> {
        @Override
        public void map(YTreeMapNode entry, Yield<YTreeMapNode> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getString("name");

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(name)
                    .key("name_length").value(name.length())
                    .buildMap();

            yield.yield(outputRow);
        }
    }

    public static class SimpleReducerYTreeMapNode implements ReducerWithKey<YTreeMapNode, YTreeMapNode, String> {
        @Override
        public String key(YTreeMapNode entry) {
            return entry.getString("name");
        }

        @Override
        public void reduce(String key, Iterator<YTreeMapNode> input, Yield<YTreeMapNode> yield, Statistics statistics) {
            int sumNameLength = 0;
            while (input.hasNext()) {
                YTreeMapNode row = input.next();
                sumNameLength += row.getInt("name_length");
            }

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(key)
                    .key("sum_name_length").value(sumNameLength)
                    .buildMap();

            yield.yield(outputRow);
        }
    }

    @Entity
    private static class MapInputType {
        String name;
    }

    @Entity
    private static class MapOutputType {
        String name;
        @Column(name = "name_length")
        int nameLength;
    }

    @Entity
    private static class ReduceOutputType {
        String name;
        @Column(name = "sum_name_length")
        int sumNameLength;
    }

    public static class SimpleMapperEntity implements Mapper<MapInputType, MapOutputType> {
        @Override
        public void map(MapInputType entry, Yield<MapOutputType> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.name;

            MapOutputType mapOutputType = new MapOutputType();
            mapOutputType.name = name;
            mapOutputType.nameLength = name.length();

            yield.yield(mapOutputType);
        }
    }

    public static class SimpleReducerEntity implements ReducerWithKey<MapOutputType, ReduceOutputType, String> {
        @Override
        public String key(MapOutputType entry) {
            return entry.name;
        }

        @Override
        public void reduce(String key, Iterator<MapOutputType> input, Yield<ReduceOutputType> yield,
                           Statistics statistics) {
            int sumNameLength = 0;
            while (input.hasNext()) {
                MapOutputType row = input.next();
                sumNameLength += row.nameLength;
            }

            ReduceOutputType reduceOutputType = new ReduceOutputType();
            reduceOutputType.name = key;
            reduceOutputType.sumNameLength = sumNameLength;

            yield.yield(reduceOutputType);
        }
    }

    public static class SimpleMapperProto implements Mapper<MapInputProtoType, MapOutputProtoType> {
        @Override
        public void map(MapInputProtoType entry, Yield<MapOutputProtoType> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getName();

            yield.yield(
                    MapOutputProtoType.newBuilder()
                            .setName(name)
                            .setNameLength(name.length())
                            .build()
            );
        }
    }

    public static class SimpleReducerProto implements ReducerWithKey<MapOutputProtoType, ReduceOutputProtoType,
            String> {
        @Override
        public String key(MapOutputProtoType entry) {
            return entry.getName();
        }

        @Override
        public void reduce(String key, Iterator<MapOutputProtoType> input, Yield<ReduceOutputProtoType> yield,
                           Statistics statistics) {
            int sumNameLength = 0;
            while (input.hasNext()) {
                var row = input.next();
                sumNameLength += row.getNameLength();
            }

            yield.yield(
                    ReduceOutputProtoType.newBuilder()
                            .setName(key)
                            .setSumNameLength(sumNameLength)
                            .build()
            );
        }
    }

    @Test
    public void testSimpleMapReduceWithYTreeMapNode() {
        testSimple(new SimpleMapperYTreeMapNode(), new SimpleReducerYTreeMapNode());
    }

    @Test
    public void testSimpleMapReduceWithEntity() {
        testSimple(new SimpleMapperEntity(), new SimpleReducerEntity());
    }

    @Test
    public void testSimpleMapReduceWithProto() {
        testSimple(new SimpleMapperProto(), new SimpleReducerProto());
    }

    private void testSimple(Mapper<?, ?> mapper, Reducer<?, ?> reducer) {
        var ytFixture = createYtFixture();
        var yt = ytFixture.getYt();
        var inputTable = ytFixture.getTestDirectory().child("input-table");
        var outputTable = ytFixture.getTestDirectory().child("output-table");

        var tableSchema = createTableSchema();

        writeTable(yt, inputTable, tableSchema, List.of(
                YTree.builder().beginMap()
                        .key("name").value("aa")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("aa")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .buildMap()
        ));

        Operation op = yt.mapReduce(MapReduceOperation.builder()
                .setSpec(MapReduceSpec.builder()
                        .setReducerSpec(new ReducerSpec(reducer))
                        .setMapperSpec(new MapperSpec(mapper))
                        .setInputTables(inputTable)
                        .setOutputTables(outputTable)
                        .setReduceBy("name")
                        .setSortBy("name")
                        .build())
                .build()
        ).join();

        Assert.assertEquals(OperationStatus.COMPLETED, op.getStatus().join());

        List<YTreeMapNode> result = readTable(yt, outputTable);

        List<YTreeMapNode> expected = List.of(
                YTree.builder().beginMap()
                        .key("name").value("aa")
                        .key("sum_name_length").value(4)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .key("sum_name_length").value(1)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .key("sum_name_length").value(9)
                        .buildMap()
        );

        Assert.assertEquals(expected, result);
    }

    private TableSchema createTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("name", ColumnValueType.STRING);
        return builder.build().toWrite();
    }
}
