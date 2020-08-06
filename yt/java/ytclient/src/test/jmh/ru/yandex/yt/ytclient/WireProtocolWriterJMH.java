package ru.yandex.yt.ytclient;

import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeStateSupport;
import ru.yandex.yt.ytclient.object.LargeFlattenObjectClass;
import ru.yandex.yt.ytclient.object.LargeFlattenPrimitiveClass;
import ru.yandex.yt.ytclient.object.LargeObjectClass;
import ru.yandex.yt.ytclient.object.LargeObjectClassWithStateSupport;
import ru.yandex.yt.ytclient.object.LargeObjectWithListClass;
import ru.yandex.yt.ytclient.object.LargePrimitiveClass;
import ru.yandex.yt.ytclient.object.LargeUnflattenObjectClass;
import ru.yandex.yt.ytclient.object.LargeUnflattenObjectClassWithStateSupport;
import ru.yandex.yt.ytclient.object.LargeUnflattenPrimitiveClass;
import ru.yandex.yt.ytclient.object.SmallObjectClass;
import ru.yandex.yt.ytclient.object.SmallObjectClassWithStateSupport;
import ru.yandex.yt.ytclient.object.SmallPrimitiveClass;
import ru.yandex.yt.ytclient.wire.UnversionedRow;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 1, time = 5)
@Measurement(iterations = 3, time = 5)
public class WireProtocolWriterJMH {

    /*
Benchmark                                                                                                                   Mode  Cnt         Score         Error   Units
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport                                                 thrpt    6      3862.209 ±     799.890   ops/s
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.alloc.rate                                  thrpt    6      1561.544 ±     322.769  MB/sec
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.alloc.rate.norm                             thrpt    6    466648.024 ±       0.008    B/op
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.churn.G1_Eden_Space                         thrpt    6      1567.748 ±     328.253  MB/sec
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.churn.G1_Eden_Space.norm                    thrpt    6    468496.910 ±   15420.320    B/op
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.churn.G1_Old_Gen                            thrpt    6         0.501 ±       0.292  MB/sec
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.churn.G1_Old_Gen.norm                       thrpt    6       149.343 ±      77.374    B/op
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.count                                       thrpt    6       279.000                counts
WireProtocolWriterJMH.test0101_serializeMappedSmallObjectsWithStateSupport:·gc.time                                        thrpt    6       261.000                    ms
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState                                        thrpt    6      8433.315 ±    1297.572   ops/s
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.alloc.rate                         thrpt    6       292.229 ±      44.880  MB/sec
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.alloc.rate.norm                    thrpt    6     40000.010 ±       0.002    B/op
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.churn.G1_Eden_Space                thrpt    6       294.363 ±      62.935  MB/sec
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.churn.G1_Eden_Space.norm           thrpt    6     40268.974 ±    4121.600    B/op
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.churn.G1_Old_Gen                   thrpt    6         0.001 ±       0.002  MB/sec
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.churn.G1_Old_Gen.norm              thrpt    6         0.110 ±       0.246    B/op
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.count                              thrpt    6        64.000                counts
WireProtocolWriterJMH.test0102_serializeMappedSmallObjectsWithStateSupportSaveState:·gc.time                               thrpt    6        59.000                    ms
WireProtocolWriterJMH.test01_serializeMappedSmallObjects                                                                   thrpt    6      2814.433 ±     586.631   ops/s
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.alloc.rate                                                    thrpt    6      1854.911 ±     386.242  MB/sec
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.alloc.rate.norm                                               thrpt    6    760856.030 ±       0.009    B/op
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Eden_Space                                           thrpt    6      1863.085 ±     408.430  MB/sec
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Eden_Space.norm                                      thrpt    6    764053.513 ±   22856.475    B/op
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Old_Gen                                              thrpt    6         0.121 ±       0.309  MB/sec
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Old_Gen.norm                                         thrpt    6        47.033 ±     117.141    B/op
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.count                                                         thrpt    6       268.000                counts
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.time                                                          thrpt    6       294.000                    ms
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects                                                             thrpt    6      1077.372 ±     246.556   ops/s
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.alloc.rate                                              thrpt    6      1880.799 ±     429.189  MB/sec
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.alloc.rate.norm                                         thrpt    6   2015020.078 ±      36.844    B/op
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Eden_Space                                     thrpt    6      1887.225 ±     434.076  MB/sec
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Eden_Space.norm                                thrpt    6   2021986.895 ±   79320.371    B/op
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Old_Gen                                        thrpt    6         0.423 ±       0.372  MB/sec
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Old_Gen.norm                                   thrpt    6       445.593 ±     299.022    B/op
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.count                                                   thrpt    6       229.000                counts
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.time                                                    thrpt    6       283.000                    ms
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects                                                              thrpt    6      4165.764 ±    2733.418   ops/s
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.alloc.rate                                               thrpt    6      2659.674 ±    1743.521  MB/sec
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.alloc.rate.norm                                          thrpt    6    736888.022 ±       0.013    B/op
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Eden_Space                                      thrpt    6      2675.563 ±    1736.199  MB/sec
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Eden_Space.norm                                 thrpt    6    741712.741 ±   30800.690    B/op
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Old_Gen                                         thrpt    6         0.016 ±       0.046  MB/sec
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Old_Gen.norm                                    thrpt    6         4.624 ±      13.432    B/op
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.count                                                    thrpt    6       264.000                counts
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.time                                                     thrpt    6       321.000                    ms
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives                                                                thrpt    6      2801.383 ±      83.150   ops/s
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.alloc.rate                                                 thrpt    6      2039.400 ±      62.971  MB/sec
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.alloc.rate.norm                                            thrpt    6    840856.031 ±       0.003    B/op
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Eden_Space                                        thrpt    6      2058.407 ±      74.281  MB/sec
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                                   thrpt    6    848722.957 ±   27337.700    B/op
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Old_Gen                                           thrpt    6         0.186 ±       0.525  MB/sec
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                                      thrpt    6        76.884 ±     217.703    B/op
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.count                                                      thrpt    6       203.000                counts
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.time                                                       thrpt    6       261.000                    ms
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives                                                          thrpt    6      1019.067 ±     110.207   ops/s
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.alloc.rate                                           thrpt    6      1848.492 ±     200.035  MB/sec
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.alloc.rate.norm                                      thrpt    6   2095020.082 ±      36.853    B/op
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Eden_Space                                  thrpt    6      1847.015 ±     223.593  MB/sec
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                             thrpt    6   2093083.038 ±   52296.496    B/op
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Old_Gen                                     thrpt    6         0.785 ±       1.221  MB/sec
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                                thrpt    6       906.077 ±    1481.993    B/op
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.count                                                thrpt    6       263.000                counts
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.time                                                 thrpt    6       315.000                    ms
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives                                                           thrpt    6      4946.248 ±     587.242   ops/s
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.alloc.rate                                            thrpt    6      3156.172 ±     376.139  MB/sec
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.alloc.rate.norm                                       thrpt    6    736888.019 ±       0.007    B/op
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Eden_Space                                   thrpt    6      3173.245 ±     362.149  MB/sec
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                              thrpt    6    740947.784 ±   18965.838    B/op
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Old_Gen                                      thrpt    6         0.203 ±       0.595  MB/sec
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                                 thrpt    6        49.293 ±     144.731    B/op
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.count                                                 thrpt    6       339.000                counts
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.time                                                  thrpt    6       416.000                    ms
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport                                                 thrpt    6      2028.066 ±      80.051   ops/s
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.alloc.rate                                  thrpt    6       819.872 ±      32.218  MB/sec
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.alloc.rate.norm                             thrpt    6    466648.042 ±       0.005    B/op
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.churn.G1_Eden_Space                         thrpt    6       822.316 ±      25.303  MB/sec
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.churn.G1_Eden_Space.norm                    thrpt    6    468059.670 ±    7045.440    B/op
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.churn.G1_Old_Gen                            thrpt    6         0.216 ±       0.095  MB/sec
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.churn.G1_Old_Gen.norm                       thrpt    6       123.133 ±      53.691    B/op
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.count                                       thrpt    6       144.000                counts
WireProtocolWriterJMH.test0701_serializeMappedLargeObjectsWithStateSupport:·gc.time                                        thrpt    6       146.000                    ms
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState                                        thrpt    6      2107.366 ±      64.332   ops/s
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.alloc.rate                         thrpt    6       248.305 ±       7.510  MB/sec
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.alloc.rate.norm                    thrpt    6    136000.040 ±       0.004    B/op
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.churn.G1_Eden_Space                thrpt    6       248.094 ±       0.949  MB/sec
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.churn.G1_Eden_Space.norm           thrpt    6    135898.431 ±    4324.194    B/op
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.churn.G1_Old_Gen                   thrpt    6         0.002 ±       0.003  MB/sec
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.churn.G1_Old_Gen.norm              thrpt    6         0.987 ±       1.487    B/op
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.count                              thrpt    6        48.000                counts
WireProtocolWriterJMH.test0702_serializeMappedLargeObjectsWithStateSupportSaveState:·gc.time                               thrpt    6        49.000                    ms
WireProtocolWriterJMH.test07_serializeMappedLargeObjects                                                                   thrpt    6       545.652 ±      22.256   ops/s
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.alloc.rate                                                    thrpt    6      1542.486 ±      62.573  MB/sec
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.alloc.rate.norm                                               thrpt    6   3261848.157 ±       0.018    B/op
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Eden_Space                                           thrpt    6       447.121 ±      21.717  MB/sec
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Eden_Space.norm                                      thrpt    6    945506.845 ±   23176.443    B/op
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Old_Gen                                              thrpt    6      1977.378 ±      96.410  MB/sec
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Old_Gen.norm                                         thrpt    6   4181474.702 ±  104361.020    B/op
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.count                                                         thrpt    6       284.000                counts
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.time                                                          thrpt    6       303.000                    ms
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects                                                             thrpt    6       225.945 ±      40.213   ops/s
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.alloc.rate                                              thrpt    6      1560.949 ±     276.961  MB/sec
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.alloc.rate.norm                                         thrpt    6   7972016.101 ±      34.191    B/op
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Eden_Space                                     thrpt    6      1111.285 ±     218.415  MB/sec
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Eden_Space.norm                                thrpt    6   5673604.046 ±  171241.100    B/op
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Old_Gen                                        thrpt    6       819.833 ±     164.708  MB/sec
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Old_Gen.norm                                   thrpt    6   4185277.584 ±  141324.729    B/op
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.count                                                   thrpt    6       151.000                counts
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.time                                                    thrpt    6       154.000                    ms
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects                                                              thrpt    6       793.041 ±      15.746   ops/s
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.alloc.rate                                               thrpt    6      2004.594 ±      38.944  MB/sec
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.alloc.rate.norm                                          thrpt    6   2917904.106 ±       0.013    B/op
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Eden_Space                                      thrpt    6       407.640 ±       7.337  MB/sec
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Eden_Space.norm                                 thrpt    6    593367.582 ±    3863.454    B/op
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Old_Gen                                         thrpt    6      2858.290 ±      59.822  MB/sec
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Old_Gen.norm                                    thrpt    6   4160540.883 ±   12182.544    B/op
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.count                                                    thrpt    6       383.000                counts
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.time                                                     thrpt    6       332.000                    ms
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives                                                                thrpt    6       520.737 ±      63.538   ops/s
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.alloc.rate                                                 thrpt    6      1652.257 ±     203.187  MB/sec
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.alloc.rate.norm                                            thrpt    6   3661848.163 ±       0.031    B/op
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Eden_Space                                        thrpt    6       603.957 ±      77.223  MB/sec
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Eden_Space.norm                                   thrpt    6   1338489.674 ±   31604.285    B/op
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Old_Gen                                           thrpt    6      1884.756 ±     247.734  MB/sec
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Old_Gen.norm                                      thrpt    6   4176792.780 ±   99272.521    B/op
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.count                                                      thrpt    6       332.000                counts
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.time                                                       thrpt    6       338.000                    ms
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives                                                          thrpt    6       219.778 ±      55.202   ops/s
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.alloc.rate                                           thrpt    6      1594.359 ±     401.035  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.alloc.rate.norm                                      thrpt    6   8372014.971 ±      32.943    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Eden_Space                                  thrpt    6      1152.420 ±     302.634  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Eden_Space.norm                             thrpt    6   6051036.978 ±  392357.547    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Old_Gen                                     thrpt    6       793.259 ±     220.391  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Old_Gen.norm                                thrpt    6   4163439.318 ±  281628.742    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Survivor_Space                              thrpt    6         0.424 ±       1.146  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Survivor_Space.norm                         thrpt    6      2083.207 ±    5518.648    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.count                                                thrpt    6       154.000                counts
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.time                                                 thrpt    6       234.000                    ms
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives                                                           thrpt    6       837.650 ±      24.369   ops/s
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.alloc.rate                                            thrpt    6      2117.749 ±      62.144  MB/sec
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.alloc.rate.norm                                       thrpt    6   2917904.102 ±       0.013    B/op
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Eden_Space                                   thrpt    6       431.560 ±      24.078  MB/sec
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Eden_Space.norm                              thrpt    6    594569.348 ±   16052.233    B/op
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Old_Gen                                      thrpt    6      3021.126 ±      75.382  MB/sec
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Old_Gen.norm                                 thrpt    6   4162698.701 ±   56304.332    B/op
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.count                                                 thrpt    6       386.000                counts
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.time                                                  thrpt    6       390.000                    ms
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects                                                            thrpt    6       496.385 ±      60.747   ops/s
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.alloc.rate                                             thrpt    6      1402.069 ±     172.046  MB/sec
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.alloc.rate.norm                                        thrpt    6   3261848.167 ±       0.019    B/op
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space                                    thrpt    6       403.194 ±      49.545  MB/sec
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm                               thrpt    6    938043.813 ±   25535.569    B/op
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen                                       thrpt    6      1786.844 ±     213.074  MB/sec
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm                                  thrpt    6   4157347.295 ±  113632.451    B/op
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.count                                                  thrpt    6       293.000                counts
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.time                                                   thrpt    6       318.000                    ms
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects                                                      thrpt    6       221.480 ±      47.618   ops/s
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.alloc.rate                                       thrpt    6      1529.006 ±     330.407  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.alloc.rate.norm                                  thrpt    6   7972012.408 ±      36.871    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space                              thrpt    6      1082.900 ±     253.553  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm                         thrpt    6   5645672.300 ±  474133.041    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen                                 thrpt    6       799.542 ±     186.900  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm                            thrpt    6   4168408.001 ±  347590.251    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.count                                            thrpt    6       149.000                counts
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.time                                             thrpt    6       180.000                    ms
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects                                                       thrpt    6       771.806 ±     144.004   ops/s
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.alloc.rate                                        thrpt    6      1950.836 ±     365.692  MB/sec
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.alloc.rate.norm                                   thrpt    6   2917904.107 ±       0.025    B/op
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Eden_Space                               thrpt    6       395.933 ±      61.866  MB/sec
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm                          thrpt    6    592588.566 ±   22533.375    B/op
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Old_Gen                                  thrpt    6      2788.600 ±     504.147  MB/sec
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm                             thrpt    6   4171567.213 ±   70738.655    B/op
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.count                                             thrpt    6       377.000                counts
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.time                                              thrpt    6       380.000                    ms
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives                                                         thrpt    6       527.762 ±      52.529   ops/s
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.alloc.rate                                          thrpt    6      1673.359 ±     165.601  MB/sec
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.alloc.rate.norm                                     thrpt    6   3661848.161 ±       0.020    B/op
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space                                 thrpt    6       609.768 ±      58.656  MB/sec
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm                            thrpt    6   1334454.469 ±   35168.999    B/op
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                                    thrpt    6      1909.444 ±     175.433  MB/sec
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm                               thrpt    6   4178878.755 ±   91552.369    B/op
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.count                                               thrpt    6       299.000                counts
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.time                                                thrpt    6       318.000                    ms
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives                                                   thrpt    6       230.912 ±       8.046   ops/s
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.alloc.rate                                    thrpt    6      1674.644 ±      56.894  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.alloc.rate.norm                               thrpt    6   8372024.403 ±       0.161    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space                           thrpt    6      1206.150 ±     139.435  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm                      thrpt    6   6029253.621 ±  610646.070    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                              thrpt    6       834.254 ±      96.283  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm                         thrpt    6   4170235.792 ±  421288.424    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space                       thrpt    6         0.877 ±       0.382  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space.norm                  thrpt    6      4386.229 ±    1996.053    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.count                                         thrpt    6        97.000                counts
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.time                                          thrpt    6       161.000                    ms
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives                                                    thrpt    6       652.235 ±     312.045   ops/s
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.alloc.rate                                     thrpt    6      1649.010 ±     788.991  MB/sec
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.alloc.rate.norm                                thrpt    6   2917904.134 ±       0.057    B/op
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space                            thrpt    6       338.536 ±     155.392  MB/sec
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm                       thrpt    6    599671.285 ±   19625.073    B/op
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                               thrpt    6      2351.948 ±    1138.404  MB/sec
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm                          thrpt    6   4160746.441 ±  105644.435    B/op
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.count                                          thrpt    6       354.000                counts
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.time                                           thrpt    6       302.000                    ms
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport                                        thrpt    6      3257.821 ±     264.730   ops/s
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.alloc.rate                         thrpt    6      1272.077 ±     103.641  MB/sec
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.alloc.rate.norm                    thrpt    6    450648.026 ±       0.003    B/op
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.churn.G1_Eden_Space                thrpt    6      1285.391 ±     115.140  MB/sec
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.churn.G1_Eden_Space.norm           thrpt    6    455345.980 ±   10982.904    B/op
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.churn.G1_Old_Gen                   thrpt    6         0.209 ±       0.627  MB/sec
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.churn.G1_Old_Gen.norm              thrpt    6        72.083 ±     216.322    B/op
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.count                              thrpt    6       229.000                counts
WireProtocolWriterJMH.test1901_serializeMappedLargeUnflattenObjectsWithStateSupport:·gc.time                               thrpt    6       231.000                    ms
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState                               thrpt    6      1837.481 ±     175.903   ops/s
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.alloc.rate                thrpt    6       292.896 ±      27.966  MB/sec
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.alloc.rate.norm           thrpt    6    184000.046 ±       0.007    B/op
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.churn.G1_Eden_Space       thrpt    6       294.761 ±      65.941  MB/sec
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.churn.G1_Eden_Space.norm  thrpt    6    185096.350 ±   33435.817    B/op
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.churn.G1_Old_Gen          thrpt    6         0.004 ±       0.005  MB/sec
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.churn.G1_Old_Gen.norm     thrpt    6         2.590 ±       3.061    B/op
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.count                     thrpt    6        58.000                counts
WireProtocolWriterJMH.test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState:·gc.time                      thrpt    6        67.000                    ms
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects                                                          thrpt    6       331.796 ±      30.744   ops/s
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.alloc.rate                                           thrpt    6      3915.291 ±     363.149  MB/sec
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.alloc.rate.norm                                      thrpt    6  13621812.281 ±      36.888    B/op
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space                                  thrpt    6      3206.757 ±     275.440  MB/sec
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm                             thrpt    6  11157532.167 ±  125806.712    B/op
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                                     thrpt    6      1195.177 ±     104.819  MB/sec
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm                                thrpt    6   4158405.155 ±   43431.088    B/op
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.count                                                thrpt    6       360.000                counts
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.time                                                 thrpt    6       442.000                    ms
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects                                                    thrpt    6       119.050 ±       7.487   ops/s
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.alloc.rate                                     thrpt    6      2012.864 ±     121.069  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.alloc.rate.norm                                thrpt    6  19523988.702 ±   73760.888    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space                            thrpt    6      1762.839 ±     143.595  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm                       thrpt    6  17097639.068 ±  720670.270    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                               thrpt    6       425.814 ±      35.606  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm                          thrpt    6   4129854.201 ±  173139.775    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space                        thrpt    6         0.755 ±       0.382  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space.norm                   thrpt    6      7338.314 ±    3883.148    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.count                                          thrpt    6       239.000                counts
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.time                                           thrpt    6       313.000                    ms
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects                                                     thrpt    6      1125.877 ±      91.026   ops/s
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.alloc.rate                                      thrpt    6      3017.028 ±     245.998  MB/sec
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.alloc.rate.norm                                 thrpt    6   3093896.074 ±      24.580    B/op
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Eden_Space                             thrpt    6       576.799 ±      55.357  MB/sec
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm                        thrpt    6    591443.060 ±   17882.950    B/op
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                                thrpt    6      4061.254 ±     316.355  MB/sec
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm                           thrpt    6   4165006.554 ±  109016.133    B/op
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.count                                           thrpt    6       383.000                counts
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.time                                            thrpt    6       388.000                    ms
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives                                                       thrpt    6       317.424 ±      23.548   ops/s
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.alloc.rate                                        thrpt    6      3855.236 ±     288.351  MB/sec
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.alloc.rate.norm                                   thrpt    6  14021824.290 ±      73.735    B/op
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space                               thrpt    6      3178.564 ±     251.941  MB/sec
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm                          thrpt    6  11560858.554 ±  353681.251    B/op
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen                                  thrpt    6      1143.259 ±      91.421  MB/sec
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm                             thrpt    6   4158160.752 ±  127520.270    B/op
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.count                                             thrpt    6       327.000                counts
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.time                                              thrpt    6       423.000                    ms
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives                                                 thrpt    6       119.218 ±       8.516   ops/s
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.alloc.rate                                  thrpt    6      2089.180 ±      43.909  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.alloc.rate.norm                             thrpt    6  20243952.701 ± 1056857.930    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space                         thrpt    6      1832.318 ±      63.611  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm                    thrpt    6  17754386.183 ±  945063.981    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen                            thrpt    6       424.987 ±      35.811  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm                       thrpt    6   4116202.392 ±  111981.782    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space                     thrpt    6         2.174 ±       0.645  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space.norm                thrpt    6     21030.059 ±    5030.534    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.count                                       thrpt    6       230.000                counts
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.time                                        thrpt    6       305.000                    ms
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives                                                  thrpt    6      1166.909 ±      47.024   ops/s
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.alloc.rate                                   thrpt    6      3126.598 ±     127.753  MB/sec
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.alloc.rate.norm                              thrpt    6   3093884.071 ±      36.863    B/op
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space                          thrpt    6       603.889 ±      29.835  MB/sec
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm                     thrpt    6    597550.854 ±    7609.241    B/op
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen                             thrpt    6      4209.006 ±     180.938  MB/sec
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm                        thrpt    6   4164982.136 ±   67336.637    B/op
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.count                                        thrpt    6       540.000                counts
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.time                                         thrpt    6       446.000                    ms

     */

    @Benchmark
    public void test01_serializeMappedSmallObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallObjects.serializeMappedObjects(serializers.smallObjectsData));
    }

    @Benchmark
    public void test0101_serializeMappedSmallObjectsWithStateSupport(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallObjectsWithStateSupport.serializeMappedObjects(serializers.smallObjectsDataWithStateSupport));
    }

    @Benchmark
    public void test0102_serializeMappedSmallObjectsWithStateSupportSaveState(Serializers serializers, Blackhole blackhole) {
        for (SmallObjectClassWithStateSupport object : serializers.smallObjectsDataWithStateSupport) {
            object.saveYTreeObjectState();
            blackhole.consume(object);
        }
    }

    @Benchmark
    public void test02_serializeLegacyMappedSmallObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallObjects.serializeLegacyMappedObjects(serializers.smallObjectsData));
    }

    @Benchmark
    public void test03_serializeUnversionedSmallObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallObjects
                .serializeUnversionedObjects(serializers.smallObjectsDataUnversioned));
    }

    @Benchmark
    public void test04_serializeMappedSmallPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallPrimitives.serializeMappedObjects(serializers.smallPrimitiveData));
    }

    @Benchmark
    public void test05_serializeLegacyMappedSmallPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallPrimitives.serializeLegacyMappedObjects(serializers.smallPrimitiveData));
    }

    @Benchmark
    public void test06_serializeUnversionedSmallPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallPrimitives
                .serializeUnversionedObjects(serializers.smallPrimitiveDataUnversioned));
    }

    @Benchmark
    public void test07_serializeMappedLargeObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeObjects.serializeMappedObjects(serializers.largeObjectsData));
    }

    @Benchmark
    public void test0701_serializeMappedLargeObjectsWithStateSupport(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeObjectsWithStateSupport.serializeMappedObjects(serializers.largeObjectsDataWithStateSupport));
    }

    @Benchmark
    public void test0702_serializeMappedLargeObjectsWithStateSupportSaveState(Serializers serializers, Blackhole blackhole) {
        for (LargeObjectClassWithStateSupport object : serializers.largeObjectsDataWithStateSupport) {
            object.saveYTreeObjectState();
            blackhole.consume(object);
        }
    }

    @Benchmark
    public void test08_serializeLegacyMappedLargeObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeObjects.serializeLegacyMappedObjects(serializers.largeObjectsData));
    }

    @Benchmark
    public void test09_serializeUnversionedLargeObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeObjects
                .serializeUnversionedObjects(serializers.largeObjectsDataUnversioned));
    }

    @Benchmark
    public void test10_serializeMappedLargePrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largePrimitives.serializeMappedObjects(serializers.largePrimitiveData));
    }

    @Benchmark
    public void test11_serializeLegacyMappedLargePrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largePrimitives.serializeLegacyMappedObjects(serializers.largePrimitiveData));
    }

    @Benchmark
    public void test12_serializeUnversionedLargePrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largePrimitives
                .serializeUnversionedObjects(serializers.largePrimitiveDataUnversioned));
    }

    @Benchmark
    public void test13_serializeMappedLargeFlattenObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeFlattenObjects.serializeMappedObjects(serializers.largeFlattenObjectsData));
    }

    @Benchmark
    public void test14_serializeLegacyMappedLargeFlattenObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeFlattenObjects
                .serializeLegacyMappedObjects(serializers.largeFlattenObjectsData));
    }

    @Benchmark
    public void test15_serializeUnversionedLargeFlattenObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeFlattenObjects
                .serializeUnversionedObjects(serializers.largeFlattenObjectsDataUnversioned));
    }

    @Benchmark
    public void test16_serializeMappedLargeFlattenPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeFlattenPrimitives
                .serializeMappedObjects(serializers.largeFlattenPrimitivesData));
    }

    @Benchmark
    public void test17_serializeLegacyMappedLargeFlattenPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeFlattenPrimitives
                .serializeLegacyMappedObjects(serializers.largeFlattenPrimitivesData));
    }

    @Benchmark
    public void test18_serializeUnversionedLargeFlattenPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeFlattenPrimitives
                .serializeUnversionedObjects(serializers.largeFlattenPrimitivesDataUnversioned));
    }

    @Benchmark
    public void test19_serializeMappedLargeUnflattenObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenObjects
                .serializeMappedObjects(serializers.largeUnflattenObjectsData));
    }

    @Benchmark
    public void test1901_serializeMappedLargeUnflattenObjectsWithStateSupport(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenObjectsWithStateSupport.serializeMappedObjects(serializers.largeUnflattenObjectsDataWithStateSupport));
    }

    @Benchmark
    public void test1902_serializeMappedLargeUnflattenObjectsWithStateSupportSaveState(Serializers serializers, Blackhole blackhole) {
        for (LargeUnflattenObjectClassWithStateSupport object : serializers.largeUnflattenObjectsDataWithStateSupport) {
            object.saveYTreeObjectState();
            blackhole.consume(object);
        }
    }

    @Benchmark
    public void test20_serializeLegacyMappedLargeUnflattenObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenObjects
                .serializeLegacyMappedObjects(serializers.largeUnflattenObjectsData));
    }

    @Benchmark
    public void test21_serializeUnversionedLargeUnflattenObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenObjects
                .serializeUnversionedObjects(serializers.largeUnflattenObjectsDataUnversioned));
    }

    @Benchmark
    public void test22_serializeMappedLargeUnflattenPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenPrimitives
                .serializeMappedObjects(serializers.largeUnflattenPrimitivesData));
    }

    @Benchmark
    public void test23_serializeLegacyMappedLargeUnflattenPrimitives(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenPrimitives
                .serializeLegacyMappedObjects(serializers.largeUnflattenPrimitivesData));
    }

    @Benchmark
    public void test24_serializeUnversionedLargeUnflattenPrimitives(Serializers serializers,
                                                                    Blackhole blackhole) {
        blackhole.consume(serializers.largeUnflattenPrimitives
                .serializeUnversionedObjects(serializers.largeUnflattenPrimitivesDataUnversioned));
    }

    @Benchmark
    public void test07_serializeMappedLargeObjectsWithList(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeObjectsWithList.serializeMappedObjects(
                serializers.largeObjectsWithListData));
    }

    @Benchmark
    public void test08_serializeLegacyMappedLargeObjectsWithList(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.largeObjectsWithList.serializeLegacyMappedObjects(
                serializers.largeObjectsWithListData));
    }


    @State(Scope.Thread)
    public static class Serializers extends ForClassInstantiationJMH.ObjectMetadata {
        private final List<SmallObjectClass> smallObjectsData;
        private final List<SmallObjectClassWithStateSupport> smallObjectsDataWithStateSupport;
        private final List<UnversionedRow> smallObjectsDataUnversioned;
        private final List<SmallPrimitiveClass> smallPrimitiveData;
        private final List<UnversionedRow> smallPrimitiveDataUnversioned;
        private final List<LargeObjectClass> largeObjectsData;
        private final List<LargeObjectWithListClass> largeObjectsWithListData;
        private final List<LargeObjectClassWithStateSupport> largeObjectsDataWithStateSupport;
        private final List<UnversionedRow> largeObjectsDataUnversioned;
        private final List<LargePrimitiveClass> largePrimitiveData;
        private final List<UnversionedRow> largePrimitiveDataUnversioned;
        private final List<LargeFlattenObjectClass> largeFlattenObjectsData;
        private final List<UnversionedRow> largeFlattenObjectsDataUnversioned;
        private final List<LargeFlattenPrimitiveClass> largeFlattenPrimitivesData;
        private final List<UnversionedRow> largeFlattenPrimitivesDataUnversioned;
        private final List<LargeUnflattenObjectClass> largeUnflattenObjectsData;
        private final List<LargeUnflattenObjectClassWithStateSupport> largeUnflattenObjectsDataWithStateSupport;
        private final List<UnversionedRow> largeUnflattenObjectsDataUnversioned;
        private final List<LargeUnflattenPrimitiveClass> largeUnflattenPrimitivesData;
        private final List<UnversionedRow> largeUnflattenPrimitivesDataUnversioned;

        public Serializers() {
            smallObjectsData = smallObjects.generateObjects(1000);
            smallObjectsDataWithStateSupport = smallObjectsWithStateSupport.generateObjects(1000);
            smallObjectsDataWithStateSupport.forEach(YTreeStateSupport.saveProxy(object -> {
                // Меняем только 2 поля (плюс ключ) - все остальные не должны быть сериализованы
                object.setLongField(object.getLongField() + 1);
                object.setStringField(object.getStringField() + "+1");
            }));
            smallObjectsDataUnversioned = smallObjects.convertObjectsToUnversioned(smallObjectsData);
            smallPrimitiveData = smallPrimitives.generateObjects(1000);
            smallPrimitiveDataUnversioned = smallPrimitives.convertObjectsToUnversioned(smallPrimitiveData);
            largeObjectsData = largeObjects.generateObjects(1000);
            largeObjectsDataWithStateSupport = largeObjectsWithStateSupport.generateObjects(1000);
            largeObjectsDataWithStateSupport.forEach(YTreeStateSupport.saveProxy(object -> {
                // Меняем только 2 поля (плюс ключ) - все остальные не должны быть сериализованы
                object.setLongField1(object.getLongField1() + 1);
                object.setStringField1(object.getStringField1() + "+1");
            }));
            largeObjectsDataUnversioned = largeObjects.convertObjectsToUnversioned(largeObjectsData);
            largePrimitiveData = largePrimitives.generateObjects(1000);
            largePrimitiveDataUnversioned = largePrimitives.convertObjectsToUnversioned(largePrimitiveData);
            largeFlattenObjectsData = largeFlattenObjects.generateObjects(1000);
            largeFlattenObjectsDataUnversioned =
                    largeFlattenObjects.convertObjectsToUnversioned(largeFlattenObjectsData);
            largeFlattenPrimitivesData = largeFlattenPrimitives.generateObjects(1000);
            largeFlattenPrimitivesDataUnversioned =
                    largeFlattenPrimitives.convertObjectsToUnversioned(largeFlattenPrimitivesData);
            largeUnflattenObjectsData = largeUnflattenObjects.generateObjects(1000);
            largeUnflattenObjectsDataWithStateSupport = largeUnflattenObjectsWithStateSupport.generateObjects(1000);
            largeUnflattenObjectsDataWithStateSupport.forEach(YTreeStateSupport.saveProxy(object -> {
                // Меняем только 2 поля (плюс ключ) - все остальные не должны быть сериализованы
                object.setIntField2(object.getIntField2());
                object.setStringField1(object.getStringField1() + "+1");
            }));

            largeUnflattenObjectsDataUnversioned =
                    largeUnflattenObjects.convertObjectsToUnversioned(largeUnflattenObjectsData);
            largeUnflattenPrimitivesData = largeUnflattenPrimitives.generateObjects(1000);
            largeUnflattenPrimitivesDataUnversioned =
                    largeUnflattenPrimitives.convertObjectsToUnversioned(largeUnflattenPrimitivesData);
            largeObjectsWithListData = largeObjectsWithList.generateObjects(1000);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WireProtocolWriterJMH.class.getSimpleName() + ".*MappedLargeObjects*")
                .addProfiler(GCProfiler.class)
                .forks(2)
                .resultFormat(ResultFormatType.JSON)
                .result("writer-" + Long.toHexString(System.currentTimeMillis()) + ".json")
                .build();

        new Runner(opt).run();
    }
}
