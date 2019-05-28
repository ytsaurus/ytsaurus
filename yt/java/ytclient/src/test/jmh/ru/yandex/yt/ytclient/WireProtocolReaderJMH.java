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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class WireProtocolReaderJMH {

    /*
Benchmark                                                                                                       Mode  Cnt         Score         Error   Units
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects                                                     thrpt    6      1770.992 ±     599.125   ops/s
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.alloc.rate                                      thrpt    6       466.556 ±     157.557  MB/sec
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.alloc.rate.norm                                 thrpt    6    304040.048 ±      24.557    B/op
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.churn.G1_Eden_Space                             thrpt    6       465.828 ±     148.153  MB/sec
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.churn.G1_Eden_Space.norm                        thrpt    6    303908.046 ±   28936.285    B/op
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.churn.G1_Old_Gen                                thrpt    6         0.002 ±       0.002  MB/sec
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.churn.G1_Old_Gen.norm                           thrpt    6         1.216 ±       1.289    B/op
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.count                                           thrpt    6        84.000                counts
WireProtocolReaderJMH.test01_deserializeMappedSmallObjects:·gc.time                                            thrpt    6        79.000                    ms
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects                                               thrpt    6      1044.374 ±      30.651   ops/s
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.alloc.rate                                thrpt    6      1310.876 ±      39.400  MB/sec
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.alloc.rate.norm                           thrpt    6   1448208.078 ±       0.002    B/op
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.churn.G1_Eden_Space                       thrpt    6      1320.601 ±      49.639  MB/sec
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.churn.G1_Eden_Space.norm                  thrpt    6   1458949.726 ±   32600.639    B/op
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.churn.G1_Old_Gen                          thrpt    6         0.725 ±       0.297  MB/sec
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.churn.G1_Old_Gen.norm                     thrpt    6       800.030 ±     314.387    B/op
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.count                                     thrpt    6       218.000                counts
WireProtocolReaderJMH.test02_deserializeLegacyMappedSmallObjects:·gc.time                                      thrpt    6       267.000                    ms
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects                                                thrpt    6      3765.770 ±    1176.278   ops/s
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.alloc.rate                                 thrpt    6      1474.610 ±     460.375  MB/sec
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.alloc.rate.norm                            thrpt    6    452122.539 ±      25.571    B/op
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.churn.G1_Eden_Space                        thrpt    6      1477.792 ±     479.155  MB/sec
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.churn.G1_Eden_Space.norm                   thrpt    6    452947.192 ±   15197.676    B/op
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.churn.G1_Old_Gen                           thrpt    6         0.673 ±       0.238  MB/sec
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.churn.G1_Old_Gen.norm                      thrpt    6       206.114 ±      15.196    B/op
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.count                                      thrpt    6       221.000                counts
WireProtocolReaderJMH.test03_deserializeUnversionedSmallObjects:·gc.time                                       thrpt    6       273.000                    ms
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives                                                  thrpt    6      2093.881 ±     274.309   ops/s
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.alloc.rate                                   thrpt    6       566.110 ±      74.728  MB/sec
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.alloc.rate.norm                              thrpt    6    312048.941 ±       6.185    B/op
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.churn.G1_Eden_Space                          thrpt    6       565.701 ±      81.574  MB/sec
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                     thrpt    6    311839.778 ±   20725.297    B/op
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.churn.G1_Old_Gen                             thrpt    6         0.001 ±       0.001  MB/sec
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                        thrpt    6         0.731 ±       0.767    B/op
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.count                                        thrpt    6       123.000                counts
WireProtocolReaderJMH.test04_deserializeMappedSmallPrimitives:·gc.time                                         thrpt    6        93.000                    ms
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives                                            thrpt    6       930.479 ±     249.141   ops/s
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.alloc.rate                             thrpt    6      1220.408 ±     169.285  MB/sec
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.alloc.rate.norm                        thrpt    6   1520200.091 ±  196572.739    B/op
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.churn.G1_Eden_Space                    thrpt    6      1223.144 ±     183.400  MB/sec
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.churn.G1_Eden_Space.norm               thrpt    6   1523139.215 ±  181353.602    B/op
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.churn.G1_Old_Gen                       thrpt    6         0.241 ±       0.601  MB/sec
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                  thrpt    6       280.627 ±     670.077    B/op
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.count                                  thrpt    6       183.000                counts
WireProtocolReaderJMH.test05_deserializeLegacyMappedSmallPrimitives:·gc.time                                   thrpt    6       241.000                    ms
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives                                             thrpt    6      3078.551 ±     523.223   ops/s
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.alloc.rate                              thrpt    6      1205.870 ±     204.112  MB/sec
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.alloc.rate.norm                         thrpt    6    452120.945 ±      32.730    B/op
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.churn.G1_Eden_Space                     thrpt    6      1215.941 ±     230.010  MB/sec
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                thrpt    6    455759.914 ±   19157.642    B/op
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.churn.G1_Old_Gen                        thrpt    6         0.598 ±       0.099  MB/sec
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                   thrpt    6       225.212 ±      62.253    B/op
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.count                                   thrpt    6       200.000                counts
WireProtocolReaderJMH.test06_deserializeUnversionedSmallPrimitives:·gc.time                                    thrpt    6       237.000                    ms
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects                                                     thrpt    6       433.059 ±      25.463   ops/s
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.alloc.rate                                      thrpt    6       413.945 ±      24.043  MB/sec
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.alloc.rate.norm                                 thrpt    6   1104048.201 ±       0.036    B/op
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.churn.G1_Eden_Space                             thrpt    6       418.119 ±      31.572  MB/sec
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.churn.G1_Eden_Space.norm                        thrpt    6   1115361.863 ±   80583.551    B/op
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.churn.G1_Old_Gen                                thrpt    6         0.001 ±       0.001  MB/sec
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.churn.G1_Old_Gen.norm                           thrpt    6         3.058 ±       2.265    B/op
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.count                                           thrpt    6        91.000                counts
WireProtocolReaderJMH.test07_deserializeMappedLargeObjects:·gc.time                                            thrpt    6        70.000                    ms
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects                                               thrpt    6       188.529 ±      88.206   ops/s
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.alloc.rate                                thrpt    6       969.965 ±     317.134  MB/sec
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.alloc.rate.norm                           thrpt    6   5984221.306 ±  860122.983    B/op
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.churn.G1_Eden_Space                       thrpt    6       972.080 ±     295.062  MB/sec
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.churn.G1_Eden_Space.norm                  thrpt    6   6005577.742 ± 1060915.507    B/op
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.churn.G1_Old_Gen                          thrpt    6         0.721 ±       0.382  MB/sec
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.churn.G1_Old_Gen.norm                     thrpt    6      4635.331 ±    4448.553    B/op
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.churn.G1_Survivor_Space                   thrpt    6         1.965 ±       0.207  MB/sec
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.churn.G1_Survivor_Space.norm              thrpt    6     12291.956 ±    5377.826    B/op
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.count                                     thrpt    6       133.000                counts
WireProtocolReaderJMH.test08_deserializeLegacyMappedLargeObjects:·gc.time                                      thrpt    6       241.000                    ms
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects                                                thrpt    6       758.118 ±     261.068   ops/s
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.alloc.rate                                 thrpt    6      1210.482 ±     417.606  MB/sec
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.alloc.rate.norm                            thrpt    6   1844120.112 ±      24.536    B/op
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.churn.G1_Eden_Space                        thrpt    6      1211.997 ±     412.067  MB/sec
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.churn.G1_Eden_Space.norm                   thrpt    6   1846902.047 ±   62696.869    B/op
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.churn.G1_Old_Gen                           thrpt    6         0.997 ±       0.332  MB/sec
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.churn.G1_Old_Gen.norm                      thrpt    6      1519.612 ±      88.910    B/op
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.churn.G1_Survivor_Space                    thrpt    6         1.421 ±       0.500  MB/sec
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.churn.G1_Survivor_Space.norm               thrpt    6      2166.304 ±     262.148    B/op
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.count                                      thrpt    6       182.000                counts
WireProtocolReaderJMH.test09_deserializeUnversionedLargeObjects:·gc.time                                       thrpt    6       290.000                    ms
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives                                                  thrpt    6       409.100 ±      34.779   ops/s
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.alloc.rate                                   thrpt    6       399.608 ±      34.246  MB/sec
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.alloc.rate.norm                              thrpt    6   1128048.202 ±       0.029    B/op
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.churn.G1_Eden_Space                          thrpt    6       404.472 ±      40.171  MB/sec
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.churn.G1_Eden_Space.norm                     thrpt    6   1141905.810 ±   80023.575    B/op
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.churn.G1_Old_Gen                             thrpt    6         0.001 ±       0.001  MB/sec
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.churn.G1_Old_Gen.norm                        thrpt    6         3.700 ±       2.808    B/op
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.count                                        thrpt    6        88.000                counts
WireProtocolReaderJMH.test10_deserializeMappedLargePrimitives:·gc.time                                         thrpt    6        62.000                    ms
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives                                            thrpt    6       205.630 ±      11.888   ops/s
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.alloc.rate                             thrpt    6      1020.444 ±      59.297  MB/sec
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.alloc.rate.norm                        thrpt    6   5728219.688 ±      20.823    B/op
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.churn.G1_Eden_Space                    thrpt    6      1020.145 ±     114.961  MB/sec
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.churn.G1_Eden_Space.norm               thrpt    6   5725221.277 ±  409892.526    B/op
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.churn.G1_Old_Gen                       thrpt    6         0.269 ±       0.323  MB/sec
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.churn.G1_Old_Gen.norm                  thrpt    6      1518.979 ±    1889.145    B/op
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.churn.G1_Survivor_Space                thrpt    6         0.363 ±       0.557  MB/sec
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.churn.G1_Survivor_Space.norm           thrpt    6      2050.586 ±    3202.773    B/op
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.count                                  thrpt    6       154.000                counts
WireProtocolReaderJMH.test11_deserializeLegacyMappedLargePrimitives:·gc.time                                   thrpt    6       293.000                    ms
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives                                             thrpt    6       793.784 ±     277.814   ops/s
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.alloc.rate                              thrpt    6      1268.317 ±     443.471  MB/sec
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.alloc.rate.norm                         thrpt    6   1844128.106 ±       0.043    B/op
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.churn.G1_Eden_Space                     thrpt    6      1273.151 ±     444.424  MB/sec
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.churn.G1_Eden_Space.norm                thrpt    6   1851295.351 ±   54428.240    B/op
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.churn.G1_Old_Gen                        thrpt    6         1.043 ±       0.408  MB/sec
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.churn.G1_Old_Gen.norm                   thrpt    6      1516.668 ±     277.551    B/op
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.churn.G1_Survivor_Space                 thrpt    6         1.543 ±       0.532  MB/sec
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.churn.G1_Survivor_Space.norm            thrpt    6      2247.161 ±     338.126    B/op
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.count                                   thrpt    6       191.000                counts
WireProtocolReaderJMH.test12_deserializeUnversionedLargePrimitives:·gc.time                                    thrpt    6       296.000                    ms
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects                                              thrpt    6       358.052 ±      85.175   ops/s
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.alloc.rate                               thrpt    6       367.437 ±      87.580  MB/sec
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.alloc.rate.norm                          thrpt    6   1184033.801 ±      10.776    B/op
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space                      thrpt    6       368.050 ±      94.281  MB/sec
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm                 thrpt    6   1186336.619 ±  141550.736    B/op
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen                         thrpt    6         0.001 ±       0.001  MB/sec
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm                    thrpt    6         4.706 ±       3.651    B/op
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.count                                    thrpt    6        80.000                counts
WireProtocolReaderJMH.test13_deserializeMappedLargeFlattenObjects:·gc.time                                     thrpt    6        71.000                    ms
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects                                        thrpt    6       188.389 ±      19.326   ops/s
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.alloc.rate                         thrpt    6       939.050 ±      97.016  MB/sec
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.alloc.rate.norm                    thrpt    6   5752222.041 ±      11.782    B/op
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space                thrpt    6       947.894 ±     124.868  MB/sec
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm           thrpt    6   5805272.704 ±  318895.172    B/op
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen                   thrpt    6         1.059 ±       1.490  MB/sec
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm              thrpt    6      6592.320 ±    9785.388    B/op
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Survivor_Space            thrpt    6         1.180 ±       1.843  MB/sec
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Survivor_Space.norm       thrpt    6      7352.989 ±   12013.358    B/op
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.count                              thrpt    6       157.000                counts
WireProtocolReaderJMH.test14_deserializeLegacyMappedLargeFlattenObjects:·gc.time                               thrpt    6       280.000                    ms
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects                                         thrpt    6       746.337 ±     302.586   ops/s
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.alloc.rate                          thrpt    6      1192.775 ±     483.437  MB/sec
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.alloc.rate.norm                     thrpt    6   1844120.113 ±      24.525    B/op
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.churn.G1_Eden_Space                 thrpt    6      1193.613 ±     473.671  MB/sec
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm            thrpt    6   1846222.157 ±   60212.409    B/op
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.churn.G1_Old_Gen                    thrpt    6         0.979 ±       0.407  MB/sec
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm               thrpt    6      1515.915 ±     306.277    B/op
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.churn.G1_Survivor_Space             thrpt    6         1.393 ±       0.617  MB/sec
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.churn.G1_Survivor_Space.norm        thrpt    6      2153.517 ±     418.338    B/op
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.count                               thrpt    6       179.000                counts
WireProtocolReaderJMH.test15_deserializeUnversionedLargeFlattenObjects:·gc.time                                thrpt    6       287.000                    ms
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives                                           thrpt    6       353.418 ±      15.499   ops/s
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.alloc.rate                            thrpt    6       374.798 ±      16.501  MB/sec
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.alloc.rate.norm                       thrpt    6   1224040.973 ±      25.198    B/op
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space                   thrpt    6       372.642 ±      42.179  MB/sec
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm              thrpt    6   1217065.593 ±  132662.833    B/op
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                      thrpt    6         0.001 ±       0.001  MB/sec
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm                 thrpt    6         3.250 ±       3.832    B/op
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.count                                 thrpt    6        81.000                counts
WireProtocolReaderJMH.test16_deserializeMappedLargeFlattenPrimitives:·gc.time                                  thrpt    6        67.000                    ms
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives                                     thrpt    6       174.001 ±      56.291   ops/s
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.alloc.rate                      thrpt    6       916.235 ±     153.863  MB/sec
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.alloc.rate.norm                 thrpt    6   6112222.938 ±  982991.318    B/op
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space             thrpt    6       912.217 ±     144.887  MB/sec
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm        thrpt    6   6087418.454 ± 1037698.638    B/op
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                thrpt    6         0.894 ±       0.540  MB/sec
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm           thrpt    6      6091.579 ±    5330.824    B/op
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space         thrpt    6         1.965 ±       0.816  MB/sec
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space.norm    thrpt    6     13019.492 ±    3062.135    B/op
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.count                           thrpt    6       151.000                counts
WireProtocolReaderJMH.test17_deserializeLegacyMappedLargeFlattenPrimitives:·gc.time                            thrpt    6       249.000                    ms
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives                                      thrpt    6       805.861 ±      66.645   ops/s
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.alloc.rate                       thrpt    6      1287.884 ±     106.044  MB/sec
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.alloc.rate.norm                  thrpt    6   1844128.105 ±       0.016    B/op
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space              thrpt    6      1293.357 ±      75.404  MB/sec
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm         thrpt    6   1852483.100 ±   82919.230    B/op
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                 thrpt    6         0.999 ±       0.233  MB/sec
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm            thrpt    6      1429.580 ±     246.103    B/op
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space          thrpt    6         1.513 ±       0.415  MB/sec
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space.norm     thrpt    6      2167.158 ±     579.703    B/op
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.count                            thrpt    6       178.000                counts
WireProtocolReaderJMH.test18_deserializeUnversionedLargeFlattenPrimitives:·gc.time                             thrpt    6       278.000                    ms
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects                                            thrpt    6       166.505 ±      18.341   ops/s
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.alloc.rate                             thrpt    6      2059.689 ±     216.682  MB/sec
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.alloc.rate.norm                        thrpt    6  14276208.503 ±   61928.198    B/op
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space                    thrpt    6      2068.662 ±     211.679  MB/sec
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm               thrpt    6  14339038.201 ±  213171.501    B/op
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                       thrpt    6         0.008 ±       0.006  MB/sec
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm                  thrpt    6        56.073 ±      40.958    B/op
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.count                                  thrpt    6       283.000                counts
WireProtocolReaderJMH.test19_deserializeMappedLargeUnflattenObjects:·gc.time                                   thrpt    6       319.000                    ms
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects                                      thrpt    6       145.982 ±       8.082   ops/s
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.alloc.rate                       thrpt    6      1997.811 ±     111.521  MB/sec
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.alloc.rate.norm                  thrpt    6  15792216.566 ±      24.567    B/op
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space              thrpt    6      2004.137 ±     158.297  MB/sec
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm         thrpt    6  15840577.840 ±  518145.130    B/op
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                 thrpt    6         1.622 ±       0.447  MB/sec
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm            thrpt    6     12815.256 ±    3388.985    B/op
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space          thrpt    6         2.996 ±       0.899  MB/sec
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space.norm     thrpt    6     23674.078 ±    6759.746    B/op
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.count                            thrpt    6       251.000                counts
WireProtocolReaderJMH.test20_deserializeLegacyMappedLargeUnflattenObjects:·gc.time                             thrpt    6       408.000                    ms
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects                                       thrpt    6      1674.173 ±     729.396   ops/s
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.alloc.rate                        thrpt    6      1955.875 ±     852.657  MB/sec
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.alloc.rate.norm                   thrpt    6   1348116.051 ±      36.840    B/op
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Eden_Space               thrpt    6      1955.566 ±     853.471  MB/sec
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm          thrpt    6   1347873.139 ±   34444.398    B/op
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                  thrpt    6         1.781 ±       1.089  MB/sec
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm             thrpt    6      1284.247 ±    1274.820    B/op
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space           thrpt    6         1.574 ±       0.415  MB/sec
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space.norm      thrpt    6      1113.755 ±     691.012    B/op
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.count                             thrpt    6       265.000                counts
WireProtocolReaderJMH.test21_deserializeUnversionedLargeUnflattenObjects:·gc.time                              thrpt    6       341.000                    ms
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives                                         thrpt    6       153.363 ±      30.073   ops/s
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.alloc.rate                          thrpt    6      1899.706 ±     373.613  MB/sec
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.alloc.rate.norm                     thrpt    6  14296056.548 ±      24.470    B/op
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space                 thrpt    6      1911.656 ±     381.836  MB/sec
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm            thrpt    6  14385450.630 ±  294747.714    B/op
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen                    thrpt    6         0.013 ±       0.004  MB/sec
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm               thrpt    6       100.925 ±      32.992    B/op
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.count                               thrpt    6       286.000                counts
WireProtocolReaderJMH.test22_deserializeMappedLargeUnflattenPrimitives:·gc.time                                thrpt    6       313.000                    ms
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives                                   thrpt    6       144.903 ±       5.915   ops/s
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.alloc.rate                    thrpt    6      1990.647 ±      77.957  MB/sec
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.alloc.rate.norm               thrpt    6  15851872.570 ±   60379.934    B/op
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space           thrpt    6      1997.423 ±      69.567  MB/sec
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm      thrpt    6  15906529.145 ±  358454.113    B/op
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen              thrpt    6         0.996 ±       0.237  MB/sec
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm         thrpt    6      7930.563 ±    1714.058    B/op
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space       thrpt    6         0.877 ±       0.209  MB/sec
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space.norm  thrpt    6      6989.138 ±    1753.704    B/op
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.count                         thrpt    6       250.000                counts
WireProtocolReaderJMH.test23_deserializeLegacyMappedLargeUnflattenPrimitives:·gc.time                          thrpt    6       389.000                    ms
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives                                    thrpt    6      1657.895 ±     104.117   ops/s
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.alloc.rate                     thrpt    6      1936.816 ±     121.412  MB/sec
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.alloc.rate.norm                thrpt    6   1348120.050 ±       0.005    B/op
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space            thrpt    6      1945.747 ±      73.061  MB/sec
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm       thrpt    6   1354652.582 ±   58358.059    B/op
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen               thrpt    6         1.182 ±       0.211  MB/sec
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm          thrpt    6       821.941 ±     100.688    B/op
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space        thrpt    6         1.332 ±       0.415  MB/sec
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space.norm   thrpt    6       925.754 ±     245.488    B/op
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.count                          thrpt    6       243.000                counts
WireProtocolReaderJMH.test24_deserializeUnversionedLargeUnflattenPrimitives:·gc.time                           thrpt    6       320.000                    ms
     */

    @Benchmark
    public void test01_deserializeMappedSmallObjects(Deserializers deserializers) {
        deserializers.smallObjects.deserializeMappedObjects(deserializers.smallObjectsData);
    }

    @Benchmark
    public void test02_deserializeLegacyMappedSmallObjects(Deserializers deserializers) {
        deserializers.smallObjects.deserializeLegacyMappedObjects(deserializers.smallObjectsData);
    }

    @Benchmark
    public void test03_deserializeUnversionedSmallObjects(Deserializers deserializers, Blackhole blackhole) {
        blackhole.consume(deserializers.smallObjects.deserializeUnversionedObjects(deserializers.smallObjectsData));
    }

    @Benchmark
    public void test04_deserializeMappedSmallPrimitives(Deserializers deserializers) {
        deserializers.smallPrimitives.deserializeMappedObjects(deserializers.smallPrimitiveData);
    }

    @Benchmark
    public void test05_deserializeLegacyMappedSmallPrimitives(Deserializers deserializers) {
        deserializers.smallPrimitives.deserializeLegacyMappedObjects(deserializers.smallPrimitiveData);
    }

    @Benchmark
    public void test06_deserializeUnversionedSmallPrimitives(Deserializers deserializers, Blackhole blackhole) {
        blackhole
                .consume(deserializers.smallPrimitives.deserializeUnversionedObjects(deserializers.smallPrimitiveData));
    }

    @Benchmark
    public void test07_deserializeMappedLargeObjects(Deserializers deserializers) {
        deserializers.largeObjects.deserializeMappedObjects(deserializers.largeObjectsData);
    }

    @Benchmark
    public void test08_deserializeLegacyMappedLargeObjects(Deserializers deserializers) {
        deserializers.largeObjects.deserializeLegacyMappedObjects(deserializers.largeObjectsData);
    }

    @Benchmark
    public void test09_deserializeUnversionedLargeObjects(Deserializers deserializers, Blackhole blackhole) {
        blackhole.consume(deserializers.largeObjects.deserializeUnversionedObjects(deserializers.largeObjectsData));
    }

    @Benchmark
    public void test10_deserializeMappedLargePrimitives(Deserializers deserializers) {
        deserializers.largePrimitives.deserializeMappedObjects(deserializers.largePrimitiveData);
    }

    @Benchmark
    public void test11_deserializeLegacyMappedLargePrimitives(Deserializers deserializers) {
        deserializers.largePrimitives.deserializeLegacyMappedObjects(deserializers.largePrimitiveData);
    }

    @Benchmark
    public void test12_deserializeUnversionedLargePrimitives(Deserializers deserializers, Blackhole blackhole) {
        blackhole
                .consume(deserializers.largePrimitives.deserializeUnversionedObjects(deserializers.largePrimitiveData));
    }

    @Benchmark
    public void test13_deserializeMappedLargeFlattenObjects(Deserializers deserializers) {
        deserializers.largeFlattenObjects.deserializeMappedObjects(deserializers.largeFlattenObjectsData);
    }

    @Benchmark
    public void test14_deserializeLegacyMappedLargeFlattenObjects(Deserializers deserializers) {
        deserializers.largeFlattenObjects.deserializeLegacyMappedObjects(deserializers.largeFlattenObjectsData);
    }

    @Benchmark
    public void test15_deserializeUnversionedLargeFlattenObjects(Deserializers deserializers, Blackhole blackhole) {
        blackhole.consume(
                deserializers.largeFlattenObjects.deserializeUnversionedObjects(deserializers.largeFlattenObjectsData));
    }

    @Benchmark
    public void test16_deserializeMappedLargeFlattenPrimitives(Deserializers deserializers) {
        deserializers.largeFlattenPrimitives.deserializeMappedObjects(deserializers.largeFlattenPrimitivesData);
    }

    @Benchmark
    public void test17_deserializeLegacyMappedLargeFlattenPrimitives(Deserializers deserializers) {
        deserializers.largeFlattenPrimitives.deserializeLegacyMappedObjects(deserializers.largeFlattenPrimitivesData);
    }

    @Benchmark
    public void test18_deserializeUnversionedLargeFlattenPrimitives(Deserializers deserializers, Blackhole blackhole) {
        blackhole.consume(deserializers.largeFlattenPrimitives
                .deserializeUnversionedObjects(deserializers.largeFlattenPrimitivesData));
    }

    @Benchmark
    public void test19_deserializeMappedLargeUnflattenObjects(Deserializers deserializers) {
        deserializers.largeUnflattenObjects.deserializeMappedObjects(deserializers.largeUnflattenObjectsData);
    }

    @Benchmark
    public void test20_deserializeLegacyMappedLargeUnflattenObjects(Deserializers deserializers) {
        deserializers.largeUnflattenObjects.deserializeLegacyMappedObjects(deserializers.largeUnflattenObjectsData);
    }

    @Benchmark
    public void test21_deserializeUnversionedLargeUnflattenObjects(Deserializers deserializers, Blackhole blackhole) {
        blackhole.consume(
                deserializers.largeUnflattenObjects
                        .deserializeUnversionedObjects(deserializers.largeUnflattenObjectsData));
    }

    @Benchmark
    public void test22_deserializeMappedLargeUnflattenPrimitives(Deserializers deserializers) {
        deserializers.largeUnflattenPrimitives.deserializeMappedObjects(deserializers.largeUnflattenPrimitivesData);
    }

    @Benchmark
    public void test23_deserializeLegacyMappedLargeUnflattenPrimitives(Deserializers deserializers) {
        deserializers.largeUnflattenPrimitives
                .deserializeLegacyMappedObjects(deserializers.largeUnflattenPrimitivesData);
    }

    @Benchmark
    public void test24_deserializeUnversionedLargeUnflattenPrimitives(Deserializers deserializers,
            Blackhole blackhole)
    {
        blackhole.consume(deserializers.largeUnflattenPrimitives
                .deserializeUnversionedObjects(deserializers.largeUnflattenPrimitivesData));
    }

    @State(Scope.Thread)
    public static class Deserializers extends ForClassInstantiationJMH.ObjectMetadata {
        private final List<byte[]> smallObjectsData;
        private final List<byte[]> smallPrimitiveData;
        private final List<byte[]> largeObjectsData;
        private final List<byte[]> largePrimitiveData;
        private final List<byte[]> largeFlattenObjectsData;
        private final List<byte[]> largeFlattenPrimitivesData;
        private final List<byte[]> largeUnflattenObjectsData;
        private final List<byte[]> largeUnflattenPrimitivesData;

        public Deserializers() {
            smallObjectsData = smallObjects.generateAndSerializeObjects(1000);
            smallPrimitiveData = smallPrimitives.generateAndSerializeObjects(1000);
            largeObjectsData = largeObjects.generateAndSerializeObjects(1000);
            largePrimitiveData = largePrimitives.generateAndSerializeObjects(1000);
            largeFlattenObjectsData = largeFlattenObjects.generateAndSerializeObjects(1000);
            largeFlattenPrimitivesData = largeFlattenPrimitives.generateAndSerializeObjects(1000);
            largeUnflattenObjectsData = largeUnflattenObjects.generateAndSerializeObjects(1000);
            largeUnflattenPrimitivesData = largeUnflattenPrimitives.generateAndSerializeObjects(1000);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WireProtocolReaderJMH.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
