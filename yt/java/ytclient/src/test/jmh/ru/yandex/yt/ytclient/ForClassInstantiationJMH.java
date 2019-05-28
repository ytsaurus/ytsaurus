package ru.yandex.yt.ytclient;


import java.util.function.Consumer;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.object.LargeFlattenObjectClass;
import ru.yandex.yt.ytclient.object.LargeFlattenPrimitiveClass;
import ru.yandex.yt.ytclient.object.LargeObjectClass;
import ru.yandex.yt.ytclient.object.LargePrimitiveClass;
import ru.yandex.yt.ytclient.object.LargeUnflattenObjectClass;
import ru.yandex.yt.ytclient.object.LargeUnflattenPrimitiveClass;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.object.ObjectsMetadata;
import ru.yandex.yt.ytclient.object.SmallObjectClass;
import ru.yandex.yt.ytclient.object.SmallPrimitiveClass;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class ForClassInstantiationJMH {

    /*
Benchmark                                                                                      Mode  Cnt        Score        Error   Units
ForClassInstantiationJMH.mappedRowDeserializerLarge                                           thrpt    6   567508.298 ±  74532.063   ops/s
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.alloc.rate                            thrpt    6     1872.547 ±    244.747  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.alloc.rate.norm                       thrpt    6     3808.000 ±      0.001    B/op
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.churn.G1_Eden_Space                   thrpt    6     1867.744 ±    259.752  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.churn.G1_Eden_Space.norm              thrpt    6     3798.015 ±    128.127    B/op
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.churn.G1_Old_Gen                      thrpt    6        0.006 ±      0.005  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.churn.G1_Old_Gen.norm                 thrpt    6        0.011 ±      0.011    B/op
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.count                                 thrpt    6      235.000               counts
ForClassInstantiationJMH.mappedRowDeserializerLarge:·gc.time                                  thrpt    6      268.000                   ms
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten                                    thrpt    6   559735.898 ±  73988.666   ops/s
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.alloc.rate                     thrpt    6     1970.874 ±    272.839  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.alloc.rate.norm                thrpt    6     4064.000 ±     24.575    B/op
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.churn.G1_Eden_Space            thrpt    6     1977.883 ±    285.081  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.churn.G1_Eden_Space.norm       thrpt    6     4078.298 ±    129.616    B/op
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.churn.G1_Old_Gen               thrpt    6        0.006 ±      0.003  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.churn.G1_Old_Gen.norm          thrpt    6        0.012 ±      0.005    B/op
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.count                          thrpt    6      227.000               counts
ForClassInstantiationJMH.mappedRowDeserializerLargeFlatten:·gc.time                           thrpt    6      269.000                   ms
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten                                  thrpt    6  1583334.886 ± 116062.093   ops/s
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.alloc.rate                   thrpt    6     2281.996 ±    168.048  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.alloc.rate.norm              thrpt    6     1664.000 ±      0.001    B/op
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Eden_Space          thrpt    6     2286.778 ±    212.709  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Eden_Space.norm     thrpt    6     1667.301 ±     51.501    B/op
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Old_Gen             thrpt    6        0.007 ±      0.004  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Old_Gen.norm        thrpt    6        0.005 ±      0.003    B/op
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.count                        thrpt    6      237.000               counts
ForClassInstantiationJMH.mappedRowDeserializerLargeUnflatten:·gc.time                         thrpt    6      276.000                   ms
ForClassInstantiationJMH.mappedRowDeserializerSmall                                           thrpt    6  2679634.336 ± 295434.812   ops/s
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.alloc.rate                            thrpt    6     2526.577 ±    279.105  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.alloc.rate.norm                       thrpt    6     1088.000 ±      0.001    B/op
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.churn.G1_Eden_Space                   thrpt    6     2521.701 ±    285.832  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.churn.G1_Eden_Space.norm              thrpt    6     1085.876 ±     13.855    B/op
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.churn.G1_Old_Gen                      thrpt    6        0.009 ±      0.007  MB/sec
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.churn.G1_Old_Gen.norm                 thrpt    6        0.004 ±      0.003    B/op
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.count                                 thrpt    6      286.000               counts
ForClassInstantiationJMH.mappedRowDeserializerSmall:·gc.time                                  thrpt    6      322.000                   ms
ForClassInstantiationJMH.mappedRowSerializerLarge                                             thrpt    6   723255.132 ±  86563.276   ops/s
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.alloc.rate                              thrpt    6     2331.010 ±    278.603  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.alloc.rate.norm                         thrpt    6     3720.000 ±      0.001    B/op
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.churn.G1_Eden_Space                     thrpt    6     2319.779 ±    278.644  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.churn.G1_Eden_Space.norm                thrpt    6     3702.231 ±    111.980    B/op
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.churn.G1_Old_Gen                        thrpt    6        0.008 ±      0.003  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.churn.G1_Old_Gen.norm                   thrpt    6        0.012 ±      0.004    B/op
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.count                                   thrpt    6      241.000               counts
ForClassInstantiationJMH.mappedRowSerializerLarge:·gc.time                                    thrpt    6      287.000                   ms
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten                                      thrpt    6   585014.634 ± 108270.348   ops/s
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.alloc.rate                       thrpt    6     1944.121 ±    355.127  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.alloc.rate.norm                  thrpt    6     3836.000 ±     12.287    B/op
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.churn.G1_Eden_Space              thrpt    6     1951.002 ±    352.053  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.churn.G1_Eden_Space.norm         thrpt    6     3849.878 ±    102.669    B/op
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.churn.G1_Old_Gen                 thrpt    6        0.005 ±      0.002  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.churn.G1_Old_Gen.norm            thrpt    6        0.010 ±      0.005    B/op
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.count                            thrpt    6      222.000               counts
ForClassInstantiationJMH.mappedRowSerializerLargeFlatten:·gc.time                             thrpt    6      251.000                   ms
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten                                    thrpt    6  1888109.596 ± 154157.834   ops/s
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.alloc.rate                     thrpt    6     2473.274 ±    121.366  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.alloc.rate.norm                thrpt    6     1512.000 ±     73.724    B/op
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.churn.G1_Eden_Space            thrpt    6     2484.051 ±    191.109  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.churn.G1_Eden_Space.norm       thrpt    6     1518.365 ±     86.215    B/op
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.churn.G1_Old_Gen               thrpt    6        0.009 ±      0.009  MB/sec
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.churn.G1_Old_Gen.norm          thrpt    6        0.006 ±      0.005    B/op
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.count                          thrpt    6      284.000               counts
ForClassInstantiationJMH.mappedRowSerializerLargeUnflatten:·gc.time                           thrpt    6      323.000                   ms
ForClassInstantiationJMH.mappedRowSerializerSmall                                             thrpt    6  3190734.548 ± 210588.196   ops/s
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.alloc.rate                              thrpt    6     2632.395 ±    172.817  MB/sec
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.alloc.rate.norm                         thrpt    6      952.000 ±      0.001    B/op
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.churn.G1_Eden_Space                     thrpt    6     2629.294 ±    189.880  MB/sec
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.churn.G1_Eden_Space.norm                thrpt    6      950.850 ±     17.592    B/op
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.churn.G1_Old_Gen                        thrpt    6        0.008 ±      0.006  MB/sec
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.churn.G1_Old_Gen.norm                   thrpt    6        0.003 ±      0.002    B/op
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.count                                   thrpt    6      266.000               counts
ForClassInstantiationJMH.mappedRowSerializerSmall:·gc.time                                    thrpt    6      315.000                   ms
ForClassInstantiationJMH.serializationFactoryLarge                                            thrpt    6     4196.230 ±    997.641   ops/s
ForClassInstantiationJMH.serializationFactoryLarge:·gc.alloc.rate                             thrpt    6      146.050 ±     34.804  MB/sec
ForClassInstantiationJMH.serializationFactoryLarge:·gc.alloc.rate.norm                        thrpt    6    40165.762 ±     99.441    B/op
ForClassInstantiationJMH.serializationFactoryLarge:·gc.churn.G1_Eden_Space                    thrpt    6      130.331 ±    106.883  MB/sec
ForClassInstantiationJMH.serializationFactoryLarge:·gc.churn.G1_Eden_Space.norm               thrpt    6    35591.887 ±  24817.888    B/op
ForClassInstantiationJMH.serializationFactoryLarge:·gc.churn.G1_Survivor_Space                thrpt    6        0.606 ±      2.709  MB/sec
ForClassInstantiationJMH.serializationFactoryLarge:·gc.churn.G1_Survivor_Space.norm           thrpt    6      162.914 ±    727.331    B/op
ForClassInstantiationJMH.serializationFactoryLarge:·gc.count                                  thrpt    6        9.000               counts
ForClassInstantiationJMH.serializationFactoryLarge:·gc.time                                   thrpt    6      612.000                   ms
ForClassInstantiationJMH.serializationFactoryLargeFlatten                                     thrpt    6     1178.602 ±    283.628   ops/s
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.alloc.rate                      thrpt    6       86.172 ±     20.557  MB/sec
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.alloc.rate.norm                 thrpt    6    84368.071 ±    196.609    B/op
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.churn.G1_Eden_Space             thrpt    6       86.447 ±    112.252  MB/sec
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.churn.G1_Eden_Space.norm        thrpt    6    83815.652 ±  98565.040    B/op
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.churn.G1_Survivor_Space         thrpt    6        0.666 ±      4.098  MB/sec
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.churn.G1_Survivor_Space.norm    thrpt    6      674.898 ±   4135.159    B/op
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.count                           thrpt    6        9.000               counts
ForClassInstantiationJMH.serializationFactoryLargeFlatten:·gc.time                            thrpt    6      614.000                   ms
ForClassInstantiationJMH.serializationFactoryLargeUnflatten                                   thrpt    6     1057.412 ±    355.773   ops/s
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.alloc.rate                    thrpt    6       70.070 ±     23.699  MB/sec
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.alloc.rate.norm               thrpt    6    76452.384 ±    298.723    B/op
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.churn.G1_Eden_Space           thrpt    6       73.527 ±     59.610  MB/sec
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.churn.G1_Eden_Space.norm      thrpt    6    79916.602 ±  50335.167    B/op
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.churn.G1_Survivor_Space       thrpt    6        0.363 ±      2.012  MB/sec
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.churn.G1_Survivor_Space.norm  thrpt    6      461.488 ±   2609.674    B/op
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.count                         thrpt    6        7.000               counts
ForClassInstantiationJMH.serializationFactoryLargeUnflatten:·gc.time                          thrpt    6      437.000                   ms
ForClassInstantiationJMH.serializationFactorySmall                                            thrpt    6     4315.783 ±   1422.320   ops/s
ForClassInstantiationJMH.serializationFactorySmall:·gc.alloc.rate                             thrpt    6       73.071 ±     24.139  MB/sec
ForClassInstantiationJMH.serializationFactorySmall:·gc.alloc.rate.norm                        thrpt    6    19540.692 ±     53.134    B/op
ForClassInstantiationJMH.serializationFactorySmall:·gc.churn.G1_Eden_Space                    thrpt    6       71.143 ±     54.164  MB/sec
ForClassInstantiationJMH.serializationFactorySmall:·gc.churn.G1_Eden_Space.norm               thrpt    6    18894.964 ±   9932.768    B/op
ForClassInstantiationJMH.serializationFactorySmall:·gc.churn.G1_Survivor_Space                thrpt    6        0.212 ±      1.040  MB/sec
ForClassInstantiationJMH.serializationFactorySmall:·gc.churn.G1_Survivor_Space.norm           thrpt    6       58.710 ±    290.797    B/op
ForClassInstantiationJMH.serializationFactorySmall:·gc.count                                  thrpt    6        7.000               counts
ForClassInstantiationJMH.serializationFactorySmall:·gc.time                                   thrpt    6      414.000                   ms

     */

    @Benchmark
    public void test01_serializationFactorySmall(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(SmallObjectClass.class));
    }

    @Benchmark
    public void test02_serializationFactoryLarge(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeObjectClass.class));
    }

    @Benchmark
    public void test03_serializationFactoryLargeFlatten(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeFlattenObjectClass.class));
    }

    @Benchmark
    public void test04_serializationFactoryLargeUnflatten(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeUnflattenObjectClass.class));
    }

    @Benchmark
    public void test05_mappedRowSerializerSmall(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.smallObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test06_mappedRowSerializerLarge(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test07_mappedRowSerializerLargeFlatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeFlattenObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test08_mappedRowSerializerLargeUnflatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeUnflattenObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test09_mappedRowDeserializerSmall(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.smallObjects.getTableSchema(),
                metadata.smallObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test10_mappedRowDeserializerLarge(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeObjects.getTableSchema(),
                metadata.largeObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test11_mappedRowDeserializerLargeFlatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeFlattenObjects.getTableSchema(),
                metadata.largeFlattenObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test12_mappedRowDeserializerLargeUnflatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeUnflattenObjects.getTableSchema(),
                metadata.largeUnflattenObjects.getyTreeSerializer(), blackhole::consume));
    }

    @State(Scope.Thread)
    public static class ObjectMetadata {

        final ObjectsMetadata<SmallObjectClass> smallObjects;
        final ObjectsMetadata<SmallPrimitiveClass> smallPrimitives;
        final ObjectsMetadata<LargeObjectClass> largeObjects;
        final ObjectsMetadata<LargePrimitiveClass> largePrimitives;
        final ObjectsMetadata<LargeFlattenObjectClass> largeFlattenObjects;
        final ObjectsMetadata<LargeFlattenPrimitiveClass> largeFlattenPrimitives;
        final ObjectsMetadata<LargeUnflattenObjectClass> largeUnflattenObjects;
        final ObjectsMetadata<LargeUnflattenPrimitiveClass> largeUnflattenPrimitives;

        private Consumer<Object> delegate;

        public ObjectMetadata() {
            this.smallObjects = ObjectsMetadata.getMetadata(SmallObjectClass.class,
                    value -> delegate.accept(value));
            this.smallPrimitives = ObjectsMetadata.getMetadata(SmallPrimitiveClass.class,
                    value -> delegate.accept(value));

            this.largeObjects = ObjectsMetadata.getMetadata(LargeObjectClass.class,
                    value -> delegate.accept(value));
            this.largePrimitives = ObjectsMetadata.getMetadata(LargePrimitiveClass.class,
                    value -> delegate.accept(value));

            this.largeFlattenObjects = ObjectsMetadata.getMetadata(LargeFlattenObjectClass.class,
                    value -> delegate.accept(value));
            this.largeFlattenPrimitives = ObjectsMetadata.getMetadata(LargeFlattenPrimitiveClass.class,
                    value -> delegate.accept(value));

            this.largeUnflattenObjects = ObjectsMetadata.getMetadata(LargeUnflattenObjectClass.class,
                    value -> delegate.accept(value));
            this.largeUnflattenPrimitives = ObjectsMetadata.getMetadata(LargeUnflattenPrimitiveClass.class,
                    value -> delegate.accept(value));
        }

        @Setup(Level.Trial)
        public void init(Blackhole blackhole) {
            delegate = blackhole::consume;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ForClassInstantiationJMH.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
