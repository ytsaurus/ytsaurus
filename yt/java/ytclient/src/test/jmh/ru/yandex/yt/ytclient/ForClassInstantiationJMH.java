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
import ru.yandex.yt.ytclient.object.LargeWithAllSupportedSerializersClass;
import ru.yandex.yt.ytclient.object.MappedRowSerializer;
import ru.yandex.yt.ytclient.object.MappedRowsetDeserializer;
import ru.yandex.yt.ytclient.object.ObjectsMetadata;
import ru.yandex.yt.ytclient.object.SmallObjectClass;
import ru.yandex.yt.ytclient.object.SmallPrimitiveClass;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class ForClassInstantiationJMH {

    // @formatter:off
    /*
Benchmark                                                                                      Mode  Cnt        Score
ForClassInstantiationJMH.test00_serializationFactorySmall                                                              thrpt    6     3903.453 ±   1071.039   ops/s
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.alloc.rate                                               thrpt    6       70.511 ±     19.321  MB/sec
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.alloc.rate.norm                                          thrpt    6    20847.589 ±     31.658    B/op
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Eden_Space                                      thrpt    6       73.335 ±     65.356  MB/sec
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Eden_Space.norm                                 thrpt    6    22240.493 ±  25045.374    B/op
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Survivor_Space                                  thrpt    6        0.454 ±      2.874  MB/sec
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Survivor_Space.norm                             thrpt    6      122.361 ±    769.333    B/op
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.count                                                    thrpt    6        9.000               counts
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.time                                                     thrpt    6      476.000                   ms
ForClassInstantiationJMH.test01_serializationFactoryLarge                                                              thrpt    6     3469.082 ±   1540.571   ops/s
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.alloc.rate                                               thrpt    6      126.076 ±     55.974  MB/sec
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.alloc.rate.norm                                          thrpt    6    41942.473 ±     45.947    B/op
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Eden_Space                                      thrpt    6      117.199 ±    108.602  MB/sec
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Eden_Space.norm                                 thrpt    6    39455.090 ±  34556.375    B/op
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Survivor_Space                                  thrpt    6        0.272 ±      1.634  MB/sec
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Survivor_Space.norm                             thrpt    6      110.347 ±    676.318    B/op
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.count                                                    thrpt    6       10.000               counts
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.time                                                     thrpt    6      645.000                   ms
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten                                                       thrpt    6      938.911 ±    314.698   ops/s
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.alloc.rate                                        thrpt    6       70.455 ±     23.583  MB/sec
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.alloc.rate.norm                                   thrpt    6    86592.090 ±      0.038    B/op
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Eden_Space                               thrpt    6       80.252 ±     87.718  MB/sec
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Eden_Space.norm                          thrpt    6    98215.592 ±  95005.496    B/op
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Survivor_Space                           thrpt    6        1.029 ±      4.470  MB/sec
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Survivor_Space.norm                      thrpt    6     1206.890 ±   5243.927    B/op
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.count                                             thrpt    6       10.000               counts
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.time                                              thrpt    6      532.000                   ms
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten                                                     thrpt    6      951.970 ±    437.377   ops/s
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.alloc.rate                                      thrpt    6       64.348 ±     29.591  MB/sec
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.alloc.rate.norm                                 thrpt    6    78008.679 ±    135.372    B/op
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Eden_Space                             thrpt    6       61.113 ±     56.101  MB/sec
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Eden_Space.norm                        thrpt    6    75753.015 ±  73145.807    B/op
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Survivor_Space                         thrpt    6        0.575 ±      2.564  MB/sec
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Survivor_Space.norm                    thrpt    6      634.136 ±   2694.520    B/op
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.count                                           thrpt    6        8.000               counts
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.time                                            thrpt    6      405.000                   ms
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers                                   thrpt    6      629.143 ±    159.636   ops/s
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.alloc.rate                    thrpt    6       94.560 ±     23.865  MB/sec
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.alloc.rate.norm               thrpt    6   173472.741 ±    445.150    B/op
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space           thrpt    6       93.804 ±     93.745  MB/sec
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space.norm      thrpt    6   171036.924 ± 157149.779    B/op
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Survivor_Space       thrpt    6        1.150 ±      4.003  MB/sec
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Survivor_Space.norm  thrpt    6     2264.428 ±   7944.045    B/op
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.count                         thrpt    6       12.000               counts
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.time                          thrpt    6      594.000                   ms
ForClassInstantiationJMH.test10_mappedRowSerializerSmall                                                               thrpt    6  2712661.561 ± 409988.169   ops/s
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.alloc.rate                                                thrpt    6     2180.302 ±    312.395  MB/sec
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.alloc.rate.norm                                           thrpt    6      928.000 ±     73.724    B/op
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.churn.G1_Eden_Space                                       thrpt    6     2179.944 ±    304.889  MB/sec
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.churn.G1_Eden_Space.norm                                  thrpt    6      927.977 ±     82.486    B/op
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.churn.G1_Old_Gen                                          thrpt    6        0.008 ±      0.003  MB/sec
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.churn.G1_Old_Gen.norm                                     thrpt    6        0.003 ±      0.001    B/op
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.count                                                     thrpt    6      293.000               counts
ForClassInstantiationJMH.test10_mappedRowSerializerSmall:·gc.time                                                      thrpt    6      313.000                   ms
ForClassInstantiationJMH.test11_mappedRowSerializerLarge                                                               thrpt    6   643124.540 ±  63705.359   ops/s
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.alloc.rate                                                thrpt    6     2059.832 ±    220.741  MB/sec
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.alloc.rate.norm                                           thrpt    6     3696.000 ±     73.724    B/op
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.churn.G1_Eden_Space                                       thrpt    6     2061.183 ±    217.052  MB/sec
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.churn.G1_Eden_Space.norm                                  thrpt    6     3698.626 ±    118.152    B/op
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.churn.G1_Old_Gen                                          thrpt    6        0.007 ±      0.005  MB/sec
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.churn.G1_Old_Gen.norm                                     thrpt    6        0.012 ±      0.009    B/op
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.count                                                     thrpt    6      262.000               counts
ForClassInstantiationJMH.test11_mappedRowSerializerLarge:·gc.time                                                      thrpt    6      278.000                   ms
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten                                                        thrpt    6   523325.399 ±  76678.457   ops/s
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.alloc.rate                                         thrpt    6     1740.927 ±    255.261  MB/sec
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.alloc.rate.norm                                    thrpt    6     3840.000 ±      0.001    B/op
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.churn.G1_Eden_Space                                thrpt    6     1741.929 ±    260.624  MB/sec
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.churn.G1_Eden_Space.norm                           thrpt    6     3842.165 ±    120.247    B/op
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.churn.G1_Old_Gen                                   thrpt    6        0.004 ±      0.002  MB/sec
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.churn.G1_Old_Gen.norm                              thrpt    6        0.009 ±      0.004    B/op
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.count                                              thrpt    6      181.000               counts
ForClassInstantiationJMH.test12_mappedRowSerializerLargeFlatten:·gc.time                                               thrpt    6      192.000                   ms
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten                                                      thrpt    6  1548627.649 ± 297882.424   ops/s
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.alloc.rate                                       thrpt    6     2026.995 ±    303.296  MB/sec
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.alloc.rate.norm                                  thrpt    6     1512.000 ±     73.724    B/op
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.churn.G1_Eden_Space                              thrpt    6     2040.708 ±    299.811  MB/sec
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.churn.G1_Eden_Space.norm                         thrpt    6     1522.390 ±     92.964    B/op
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.churn.G1_Old_Gen                                 thrpt    6        0.008 ±      0.006  MB/sec
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.churn.G1_Old_Gen.norm                            thrpt    6        0.006 ±      0.004    B/op
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.count                                            thrpt    6      212.000               counts
ForClassInstantiationJMH.test13_mappedRowSerializerLargeUnflatten:·gc.time                                             thrpt    6      232.000                   ms
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers                                    thrpt    6   429939.158 ±  99631.770   ops/s
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.alloc.rate                     thrpt    6     1785.039 ±    413.253  MB/sec
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.alloc.rate.norm                thrpt    6     4792.000 ±      0.001    B/op
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space            thrpt    6     1793.830 ±    408.612  MB/sec
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space.norm       thrpt    6     4816.394 ±    144.162    B/op
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.churn.G1_Old_Gen               thrpt    6        0.006 ±      0.007  MB/sec
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.churn.G1_Old_Gen.norm          thrpt    6        0.015 ±      0.017    B/op
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.count                          thrpt    6      230.000               counts
ForClassInstantiationJMH.test14_mappedRowSerializerLargeWithAllSupportedSerializers:·gc.time                           thrpt    6      237.000                   ms
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall                                                             thrpt    6  2428331.842 ± 270389.819   ops/s
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.alloc.rate                                              thrpt    6     2273.040 ±    302.704  MB/sec
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.alloc.rate.norm                                         thrpt    6     1080.000 ±     24.575    B/op
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.churn.G1_Eden_Space                                     thrpt    6     2281.480 ±    335.373  MB/sec
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.churn.G1_Eden_Space.norm                                thrpt    6     1083.858 ±     45.124    B/op
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.churn.G1_Old_Gen                                        thrpt    6        0.008 ±      0.004  MB/sec
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.churn.G1_Old_Gen.norm                                   thrpt    6        0.004 ±      0.002    B/op
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.count                                                   thrpt    6      237.000               counts
ForClassInstantiationJMH.test20_mappedRowDeserializerSmall:·gc.time                                                    thrpt    6      251.000                   ms
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge                                                             thrpt    6   527080.647 ±  75425.623   ops/s
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.alloc.rate                                              thrpt    6     1739.193 ±    249.607  MB/sec
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.alloc.rate.norm                                         thrpt    6     3808.000 ±      0.001    B/op
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.churn.G1_Eden_Space                                     thrpt    6     1740.773 ±    283.013  MB/sec
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.churn.G1_Eden_Space.norm                                thrpt    6     3810.735 ±    179.948    B/op
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.churn.G1_Old_Gen                                        thrpt    6        0.005 ±      0.002  MB/sec
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.churn.G1_Old_Gen.norm                                   thrpt    6        0.011 ±      0.006    B/op
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.count                                                   thrpt    6      198.000               counts
ForClassInstantiationJMH.test21_mappedRowDeserializerLarge:·gc.time                                                    thrpt    6      211.000                   ms
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten                                                      thrpt    6   496795.944 ± 105871.835   ops/s
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.alloc.rate                                       thrpt    6     1749.847 ±    382.624  MB/sec
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.alloc.rate.norm                                  thrpt    6     4064.000 ±     24.575    B/op
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.churn.G1_Eden_Space                              thrpt    6     1748.806 ±    397.113  MB/sec
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.churn.G1_Eden_Space.norm                         thrpt    6     4060.995 ±    143.111    B/op
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.churn.G1_Old_Gen                                 thrpt    6        0.006 ±      0.003  MB/sec
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.churn.G1_Old_Gen.norm                            thrpt    6        0.015 ±      0.010    B/op
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.count                                            thrpt    6      218.000               counts
ForClassInstantiationJMH.test22_mappedRowDeserializerLargeFlatten:·gc.time                                             thrpt    6      219.000                   ms
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten                                                    thrpt    6  1482299.008 ± 135800.604   ops/s
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.alloc.rate                                     thrpt    6     2127.465 ±    226.364  MB/sec
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.alloc.rate.norm                                thrpt    6     1656.000 ±     24.575    B/op
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Eden_Space                            thrpt    6     2127.279 ±    217.606  MB/sec
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Eden_Space.norm                       thrpt    6     1656.023 ±     60.724    B/op
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Old_Gen                               thrpt    6        0.007 ±      0.003  MB/sec
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.churn.G1_Old_Gen.norm                          thrpt    6        0.005 ±      0.003    B/op
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.count                                          thrpt    6      221.000               counts
ForClassInstantiationJMH.test23_mappedRowDeserializerLargeUnflatten:·gc.time                                           thrpt    6      227.000                   ms
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers                                  thrpt    6   350879.883 ± 165520.370   ops/s
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.alloc.rate                   thrpt    6     1481.459 ±    699.125  MB/sec
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.alloc.rate.norm              thrpt    6     4872.000 ±      0.001    B/op
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space          thrpt    6     1478.890 ±    699.713  MB/sec
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space.norm     thrpt    6     4863.891 ±    131.262    B/op
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.churn.G1_Old_Gen             thrpt    6        0.006 ±      0.003  MB/sec
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.churn.G1_Old_Gen.norm        thrpt    6        0.020 ±      0.010    B/op
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.count                        thrpt    6      267.000               counts
ForClassInstantiationJMH.test24_mappedRowDeserializerLargeWithAllSupportedSerializers:·gc.time                         thrpt    6      220.000                   ms
     */




    // Кэширование YTreeObjectSerializerFactory.forClass полностью убрало расходы на повторной создание маппингов классов
    // Можно подумать и над кэшированием классов MappedRowSerializer/MappedRowDeserializer
/*

Benchmark                                                                                                           Mode  Cnt         Score          Error   Units
ForClassInstantiationJMH.test00_serializationFactorySmall                                                          thrpt    6  81163969.365 ± 20797889.914   ops/s
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.alloc.rate                                           thrpt    6      1125.046 ±      288.532  MB/sec
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.alloc.rate.norm                                      thrpt    6        16.000 ±        0.001    B/op
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Eden_Space                                  thrpt    6      1123.525 ±      292.792  MB/sec
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Eden_Space.norm                             thrpt    6        15.978 ±        0.633    B/op
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Old_Gen                                     thrpt    6         0.003 ±        0.002  MB/sec
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.churn.G1_Old_Gen.norm                                thrpt    6        ≈ 10⁻⁴                   B/op
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.count                                                thrpt    6       168.000                 counts
ForClassInstantiationJMH.test00_serializationFactorySmall:·gc.time                                                 thrpt    6       173.000                     ms
ForClassInstantiationJMH.test01_serializationFactoryLarge                                                          thrpt    6  87455088.336 ±  2331727.550   ops/s
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.alloc.rate                                           thrpt    6      1212.426 ±       31.530  MB/sec
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.alloc.rate.norm                                      thrpt    6        16.000 ±        0.001    B/op
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Eden_Space                                  thrpt    6      1210.397 ±       46.903  MB/sec
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Eden_Space.norm                             thrpt    6        15.973 ±        0.450    B/op
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Old_Gen                                     thrpt    6         0.003 ±        0.002  MB/sec
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.churn.G1_Old_Gen.norm                                thrpt    6        ≈ 10⁻⁴                   B/op
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.count                                                thrpt    6       166.000                 counts
ForClassInstantiationJMH.test01_serializationFactoryLarge:·gc.time                                                 thrpt    6       174.000                     ms
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten                                                   thrpt    6  91826335.094 ±  9519566.544   ops/s
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.alloc.rate                                    thrpt    6      1272.882 ±      130.896  MB/sec
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.alloc.rate.norm                               thrpt    6        16.000 ±        0.001    B/op
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Eden_Space                           thrpt    6      1266.955 ±      162.642  MB/sec
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Eden_Space.norm                      thrpt    6        15.923 ±        0.862    B/op
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Old_Gen                              thrpt    6         0.002 ±        0.002  MB/sec
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.churn.G1_Old_Gen.norm                         thrpt    6        ≈ 10⁻⁵                   B/op
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.count                                         thrpt    6       158.000                 counts
ForClassInstantiationJMH.test02_serializationFactoryLargeFlatten:·gc.time                                          thrpt    6       171.000                     ms
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten                                                 thrpt    6  94331658.043 ±  1908167.086   ops/s
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.alloc.rate                                  thrpt    6      1307.875 ±       25.517  MB/sec
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.alloc.rate.norm                             thrpt    6        16.000 ±        0.001    B/op
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Eden_Space                         thrpt    6      1311.148 ±       56.917  MB/sec
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Eden_Space.norm                    thrpt    6        16.040 ±        0.682    B/op
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Old_Gen                            thrpt    6         0.002 ±        0.001  MB/sec
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.churn.G1_Old_Gen.norm                       thrpt    6        ≈ 10⁻⁵                   B/op
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.count                                       thrpt    6       196.000                 counts
ForClassInstantiationJMH.test03_serializationFactoryLargeUnflatten:·gc.time                                        thrpt    6       207.000                     ms
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers                               thrpt    6  96495720.253 ±  5045030.597   ops/s
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.alloc.rate                thrpt    6      1337.091 ±       67.231  MB/sec
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.alloc.rate.norm           thrpt    6        16.000 ±        0.001    B/op
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space       thrpt    6      1339.597 ±       88.141  MB/sec
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Eden_Space.norm  thrpt    6        16.030 ±        0.621    B/op
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Old_Gen          thrpt    6         0.002 ±        0.003  MB/sec
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.churn.G1_Old_Gen.norm     thrpt    6        ≈ 10⁻⁵                   B/op
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.count                     thrpt    6       184.000                 counts
ForClassInstantiationJMH.test04_serializationFactoryLargeWithAllSupportedSerializers:·gc.time                      thrpt    6       198.000                     ms
 */

    // @formatter:on

    @Benchmark
    public void test00_serializationFactorySmall(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(SmallObjectClass.class));
    }

    @Benchmark
    public void test01_serializationFactoryLarge(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeObjectClass.class));
    }

    @Benchmark
    public void test02_serializationFactoryLargeFlatten(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeFlattenObjectClass.class));
    }

    @Benchmark
    public void test03_serializationFactoryLargeUnflatten(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeUnflattenObjectClass.class));
    }

    @Benchmark
    public void test04_serializationFactoryLargeWithAllSupportedSerializers(Blackhole blackhole) {
        blackhole.consume(YTreeObjectSerializerFactory.forClass(LargeWithAllSupportedSerializersClass.class));
    }

    @Benchmark
    public void test10_mappedRowSerializerSmall(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.smallObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test11_mappedRowSerializerLarge(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test12_mappedRowSerializerLargeFlatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeFlattenObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test13_mappedRowSerializerLargeUnflatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeUnflattenObjects.getyTreeSerializer()));
    }

    @Benchmark
    public void test14_mappedRowSerializerLargeWithAllSupportedSerializers(ObjectMetadata metadata,
                                                                           Blackhole blackhole) {
        blackhole.consume(MappedRowSerializer.forClass(metadata.largeWithAllSupportedSerializers.getyTreeSerializer()));
    }


    @Benchmark
    public void test20_mappedRowDeserializerSmall(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.smallObjects.getTableSchema(),
                metadata.smallObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test21_mappedRowDeserializerLarge(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeObjects.getTableSchema(),
                metadata.largeObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test22_mappedRowDeserializerLargeFlatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeFlattenObjects.getTableSchema(),
                metadata.largeFlattenObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test23_mappedRowDeserializerLargeUnflatten(ObjectMetadata metadata, Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeUnflattenObjects.getTableSchema(),
                metadata.largeUnflattenObjects.getyTreeSerializer(), blackhole::consume));
    }

    @Benchmark
    public void test24_mappedRowDeserializerLargeWithAllSupportedSerializers(ObjectMetadata metadata,
                                                                             Blackhole blackhole) {
        blackhole.consume(MappedRowsetDeserializer.forClass(metadata.largeWithAllSupportedSerializers.getTableSchema(),
                metadata.largeWithAllSupportedSerializers.getyTreeSerializer(), blackhole::consume));
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
        final ObjectsMetadata<LargeWithAllSupportedSerializersClass> largeWithAllSupportedSerializers;

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

            this.largeWithAllSupportedSerializers =
                    ObjectsMetadata.getMetadata(LargeWithAllSupportedSerializersClass.class,
                            value -> delegate.accept(value));
        }

        @Setup(Level.Trial)
        public void init(Blackhole blackhole) {
            delegate = blackhole::consume;
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ForClassInstantiationJMH.class.getSimpleName() + ".test0.+")
                .addProfiler(GCProfiler.class)
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
