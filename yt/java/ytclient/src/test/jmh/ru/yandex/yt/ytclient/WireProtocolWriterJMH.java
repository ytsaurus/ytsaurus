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

import ru.yandex.yt.ytclient.object.LargeFlattenObjectClass;
import ru.yandex.yt.ytclient.object.LargeFlattenPrimitiveClass;
import ru.yandex.yt.ytclient.object.LargeObjectClass;
import ru.yandex.yt.ytclient.object.LargePrimitiveClass;
import ru.yandex.yt.ytclient.object.LargeUnflattenObjectClass;
import ru.yandex.yt.ytclient.object.LargeUnflattenPrimitiveClass;
import ru.yandex.yt.ytclient.object.SmallObjectClass;
import ru.yandex.yt.ytclient.object.SmallPrimitiveClass;
import ru.yandex.yt.ytclient.wire.UnversionedRow;

@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 3, time = 5)
public class WireProtocolWriterJMH {

    /*
Benchmark                                                                                                     Mode  Cnt         Score        Error   Units
WireProtocolWriterJMH.test01_serializeMappedSmallObjects                                                     thrpt    6      2940.888 ±    509.534   ops/s
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.alloc.rate                                      thrpt    6      1939.517 ±    335.768  MB/sec
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.alloc.rate.norm                                 thrpt    6    760864.029 ±      0.005    B/op
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Eden_Space                             thrpt    6      1952.534 ±    335.164  MB/sec
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Eden_Space.norm                        thrpt    6    765998.317 ±  10355.969    B/op
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Old_Gen                                thrpt    6         0.081 ±      0.418  MB/sec
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.churn.G1_Old_Gen.norm                           thrpt    6        33.720 ±    175.355    B/op
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.count                                           thrpt    6       279.000               counts
WireProtocolWriterJMH.test01_serializeMappedSmallObjects:·gc.time                                            thrpt    6       320.000                   ms
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects                                               thrpt    6      1114.245 ±     19.264   ops/s
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.alloc.rate                                thrpt    6      1945.738 ±     33.321  MB/sec
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.alloc.rate.norm                           thrpt    6   2015040.075 ±      0.008    B/op
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Eden_Space                       thrpt    6      1953.954 ±     65.447  MB/sec
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Eden_Space.norm                  thrpt    6   2023598.800 ±  73061.508    B/op
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Old_Gen                          thrpt    6         0.033 ±      0.037  MB/sec
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.churn.G1_Old_Gen.norm                     thrpt    6        34.415 ±     37.955    B/op
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.count                                     thrpt    6       203.000               counts
WireProtocolWriterJMH.test02_serializeLegacyMappedSmallObjects:·gc.time                                      thrpt    6       268.000                   ms
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects                                                thrpt    6      3847.764 ±   2956.632   ops/s
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.alloc.rate                                 thrpt    6      2457.277 ±   1888.558  MB/sec
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.alloc.rate.norm                            thrpt    6    736896.024 ±      0.015    B/op
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Eden_Space                        thrpt    6      2470.072 ±   1872.794  MB/sec
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Eden_Space.norm                   thrpt    6    741437.011 ±  15871.512    B/op
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Old_Gen                           thrpt    6         0.013 ±      0.026  MB/sec
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.churn.G1_Old_Gen.norm                      thrpt    6         4.205 ±     10.480    B/op
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.count                                      thrpt    6       332.000               counts
WireProtocolWriterJMH.test03_serializeUnversionedSmallObjects:·gc.time                                       thrpt    6       383.000                   ms
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives                                                  thrpt    6      2451.599 ±    202.372   ops/s
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.alloc.rate                                   thrpt    6      1786.463 ±    146.084  MB/sec
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.alloc.rate.norm                              thrpt    6    840864.034 ±      0.004    B/op
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Eden_Space                          thrpt    6      1797.473 ±    163.027  MB/sec
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                     thrpt    6    846063.632 ±  36941.704    B/op
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Old_Gen                             thrpt    6         0.120 ±      0.336  MB/sec
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                        thrpt    6        55.083 ±    153.591    B/op
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.count                                        thrpt    6       222.000               counts
WireProtocolWriterJMH.test04_serializeMappedSmallPrimitives:·gc.time                                         thrpt    6       250.000                   ms
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives                                            thrpt    6       848.107 ±    112.200   ops/s
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.alloc.rate                             thrpt    6      1579.720 ±    140.860  MB/sec
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.alloc.rate.norm                        thrpt    6   2151016.100 ± 172022.642    B/op
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Eden_Space                    thrpt    6      1578.603 ±    129.640  MB/sec
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Eden_Space.norm               thrpt    6   2149801.312 ± 191043.903    B/op
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Old_Gen                       thrpt    6         0.643 ±      0.941  MB/sec
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                  thrpt    6       889.055 ±   1369.167    B/op
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.count                                  thrpt    6       261.000               counts
WireProtocolWriterJMH.test05_serializeLegacyMappedSmallPrimitives:·gc.time                                   thrpt    6       326.000                   ms
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives                                             thrpt    6      3734.003 ±   3361.826   ops/s
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.alloc.rate                              thrpt    6      2384.868 ±   2147.560  MB/sec
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.alloc.rate.norm                         thrpt    6    736896.026 ±      0.024    B/op
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Eden_Space                     thrpt    6      2401.978 ±   2145.953  MB/sec
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Eden_Space.norm                thrpt    6    742849.329 ±  16730.435    B/op
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Old_Gen                        thrpt    6         0.020 ±      0.037  MB/sec
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.churn.G1_Old_Gen.norm                   thrpt    6         6.272 ±     12.893    B/op
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.count                                   thrpt    6       329.000               counts
WireProtocolWriterJMH.test06_serializeUnversionedSmallPrimitives:·gc.time                                    thrpt    6       365.000                   ms
WireProtocolWriterJMH.test07_serializeMappedLargeObjects                                                     thrpt    6       504.217 ±     62.095   ops/s
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.alloc.rate                                      thrpt    6      1271.521 ±    156.505  MB/sec
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.alloc.rate.norm                                 thrpt    6   2909856.170 ±      0.037    B/op
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Eden_Space                             thrpt    6       335.204 ±     28.524  MB/sec
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Eden_Space.norm                        thrpt    6    767620.626 ±  50051.733    B/op
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Old_Gen                                thrpt    6      1814.742 ±    186.985  MB/sec
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.churn.G1_Old_Gen.norm                           thrpt    6   4154223.985 ± 119829.913    B/op
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.count                                           thrpt    6       326.000               counts
WireProtocolWriterJMH.test07_serializeMappedLargeObjects:·gc.time                                            thrpt    6       338.000                   ms
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects                                               thrpt    6       219.821 ±     48.319   ops/s
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.alloc.rate                                thrpt    6      1451.535 ±    319.808  MB/sec
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.alloc.rate.norm                           thrpt    6   7620022.956 ±     32.984    B/op
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Eden_Space                       thrpt    6      1041.487 ±    234.171  MB/sec
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Eden_Space.norm                  thrpt    6   5466861.739 ±  52547.540    B/op
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Old_Gen                          thrpt    6       789.438 ±    181.698  MB/sec
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.churn.G1_Old_Gen.norm                     thrpt    6   4143315.582 ±  53982.177    B/op
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.count                                     thrpt    6       219.000               counts
WireProtocolWriterJMH.test08_serializeLegacyMappedLargeObjects:·gc.time                                      thrpt    6       192.000                   ms
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects                                                thrpt    6       681.879 ±    458.927   ops/s
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.alloc.rate                                 thrpt    6      1620.030 ±   1090.712  MB/sec
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.alloc.rate.norm                            thrpt    6   2741912.129 ±      0.088    B/op
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Eden_Space                        thrpt    6       347.347 ±    229.958  MB/sec
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Eden_Space.norm                   thrpt    6    588396.039 ±  11411.406    B/op
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Old_Gen                           thrpt    6      2454.791 ±   1650.353  MB/sec
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.churn.G1_Old_Gen.norm                      thrpt    6   4155153.132 ±  61225.204    B/op
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.count                                      thrpt    6       378.000               counts
WireProtocolWriterJMH.test09_serializeUnversionedLargeObjects:·gc.time                                       thrpt    6       440.000                   ms
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives                                                  thrpt    6       458.846 ±     49.699   ops/s
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.alloc.rate                                   thrpt    6      1316.207 ±    143.447  MB/sec
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.alloc.rate.norm                              thrpt    6   3309856.185 ±      0.024    B/op
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Eden_Space                          thrpt    6       460.768 ±     48.281  MB/sec
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Eden_Space.norm                     thrpt    6   1158833.587 ±  43888.907    B/op
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Old_Gen                             thrpt    6      1646.977 ±    207.524  MB/sec
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.churn.G1_Old_Gen.norm                        thrpt    6   4141120.756 ± 166066.904    B/op
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.count                                        thrpt    6       315.000               counts
WireProtocolWriterJMH.test10_serializeMappedLargePrimitives:·gc.time                                         thrpt    6       298.000                   ms
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives                                            thrpt    6       232.984 ±      4.120   ops/s
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.alloc.rate                             thrpt    6      1619.478 ±     29.582  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.alloc.rate.norm                        thrpt    6   8020032.400 ±      0.168    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Eden_Space                    thrpt    6      1192.659 ±     39.676  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Eden_Space.norm               thrpt    6   5906291.190 ± 153142.422    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Old_Gen                       thrpt    6       849.363 ±     28.398  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Old_Gen.norm                  thrpt    6   4206222.634 ± 111897.403    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Survivor_Space                thrpt    6         0.787 ±      0.263  MB/sec
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.churn.G1_Survivor_Space.norm           thrpt    6      3896.421 ±   1301.040    B/op
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.count                                  thrpt    6       109.000               counts
WireProtocolWriterJMH.test11_serializeLegacyMappedLargePrimitives:·gc.time                                   thrpt    6       178.000                   ms
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives                                             thrpt    6       545.498 ±     28.548   ops/s
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.alloc.rate                              thrpt    6      1296.097 ±     67.571  MB/sec
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.alloc.rate.norm                         thrpt    6   2741912.155 ±      0.030    B/op
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Eden_Space                     thrpt    6       285.669 ±     12.684  MB/sec
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Eden_Space.norm                thrpt    6    604374.333 ±  12405.963    B/op
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Old_Gen                        thrpt    6      1961.084 ±    104.142  MB/sec
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.churn.G1_Old_Gen.norm                   thrpt    6   4148740.983 ±  70897.912    B/op
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.count                                   thrpt    6       356.000               counts
WireProtocolWriterJMH.test12_serializeUnversionedLargePrimitives:·gc.time                                    thrpt    6       306.000                   ms
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects                                              thrpt    6       507.931 ±    118.924   ops/s
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.alloc.rate                               thrpt    6      1280.339 ±    301.561  MB/sec
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.alloc.rate.norm                          thrpt    6   2909856.168 ±      0.049    B/op
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space                      thrpt    6       336.777 ±     67.984  MB/sec
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm                 thrpt    6    766093.243 ±  36495.239    B/op
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen                         thrpt    6      1833.025 ±    429.271  MB/sec
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm                    thrpt    6   4166309.175 ± 129305.282    B/op
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.count                                    thrpt    6       272.000               counts
WireProtocolWriterJMH.test13_serializeMappedLargeFlattenObjects:·gc.time                                     thrpt    6       279.000                   ms
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects                                        thrpt    6       214.179 ±     54.707   ops/s
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.alloc.rate                         thrpt    6      1413.528 ±    361.667  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.alloc.rate.norm                    thrpt    6   7620022.247 ±     33.159    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space                thrpt    6      1008.611 ±    272.259  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm           thrpt    6   5436096.661 ± 321956.721    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen                   thrpt    6       770.165 ±    211.478  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm              thrpt    6   4150396.040 ± 247397.692    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Survivor_Space            thrpt    6         2.055 ±      6.456  MB/sec
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.churn.G1_Survivor_Space.norm       thrpt    6     12087.971 ±  37973.327    B/op
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.count                              thrpt    6       145.000               counts
WireProtocolWriterJMH.test14_serializeLegacyMappedLargeFlattenObjects:·gc.time                               thrpt    6       219.000                   ms
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects                                         thrpt    6       695.726 ±    498.897   ops/s
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.alloc.rate                          thrpt    6      1651.350 ±   1184.215  MB/sec
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.alloc.rate.norm                     thrpt    6   2741912.127 ±      0.093    B/op
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Eden_Space                 thrpt    6       371.171 ±    263.101  MB/sec
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Eden_Space.norm            thrpt    6    616718.196 ±  11361.872    B/op
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Old_Gen                    thrpt    6      2500.943 ±   1770.340  MB/sec
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.churn.G1_Old_Gen.norm               thrpt    6   4155750.678 ±  77414.523    B/op
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.count                               thrpt    6       426.000               counts
WireProtocolWriterJMH.test15_serializeUnversionedLargeFlattenObjects:·gc.time                                thrpt    6       377.000                   ms
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives                                           thrpt    6       457.939 ±    109.181   ops/s
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.alloc.rate                            thrpt    6      1312.466 ±    313.885  MB/sec
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.alloc.rate.norm                       thrpt    6   3309856.184 ±      0.048    B/op
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space                   thrpt    6       460.753 ±    100.395  MB/sec
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm              thrpt    6   1162629.796 ±  39193.793    B/op
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                      thrpt    6      1643.947 ±    384.238  MB/sec
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm                 thrpt    6   4146538.012 ± 108167.074    B/op
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.count                                 thrpt    6       307.000               counts
WireProtocolWriterJMH.test16_serializeMappedLargeFlattenPrimitives:·gc.time                                  thrpt    6       321.000                   ms
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives                                     thrpt    6       217.207 ±     28.501   ops/s
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.alloc.rate                      thrpt    6      1508.973 ±    195.821  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.alloc.rate.norm                 thrpt    6   8020032.425 ±      0.187    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space             thrpt    6      1107.452 ±    154.058  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm        thrpt    6   5885427.230 ± 180909.510    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                thrpt    6       782.864 ±    112.563  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm           thrpt    6   4160137.925 ± 125442.073    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space         thrpt    6         0.030 ±      0.208  MB/sec
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.churn.G1_Survivor_Space.norm    thrpt    6       154.248 ±   1059.501    B/op
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.count                           thrpt    6       211.000               counts
WireProtocolWriterJMH.test17_serializeLegacyMappedLargeFlattenPrimitives:·gc.time                            thrpt    6       269.000                   ms
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives                                      thrpt    6       768.174 ±    103.760   ops/s
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.alloc.rate                       thrpt    6      1824.378 ±    246.782  MB/sec
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.alloc.rate.norm                  thrpt    6   2741912.115 ±      0.028    B/op
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space              thrpt    6       406.238 ±     51.987  MB/sec
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Eden_Space.norm         thrpt    6    610615.085 ±   7641.034    B/op
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen                 thrpt    6      2765.210 ±    356.736  MB/sec
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.churn.G1_Old_Gen.norm            thrpt    6   4156315.570 ±  53480.931    B/op
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.count                            thrpt    6       402.000               counts
WireProtocolWriterJMH.test18_serializeUnversionedLargeFlattenPrimitives:·gc.time                             thrpt    6       441.000                   ms
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects                                            thrpt    6       326.262 ±     13.339   ops/s
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.alloc.rate                             thrpt    6      3750.973 ±    155.532  MB/sec
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.alloc.rate.norm                        thrpt    6  13269820.293 ±    110.576    B/op
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space                    thrpt    6      3099.539 ±    141.336  MB/sec
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm               thrpt    6  10965386.639 ± 271747.908    B/op
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                       thrpt    6      1171.320 ±     52.533  MB/sec
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm                  thrpt    6   4143849.822 ± 103290.056    B/op
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.count                                  thrpt    6       311.000               counts
WireProtocolWriterJMH.test19_serializeMappedLargeUnflattenObjects:·gc.time                                   thrpt    6       398.000                   ms
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects                                      thrpt    6       122.771 ±     11.111   ops/s
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.alloc.rate                       thrpt    6      2037.222 ±    168.947  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.alloc.rate.norm                  thrpt    6  19147984.690 ± 147374.332    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space              thrpt    6      1796.665 ±    162.319  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm         thrpt    6  16887638.186 ± 761254.999    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                 thrpt    6       438.646 ±     43.541  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm            thrpt    6   4122610.631 ± 176076.573    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space          thrpt    6         1.814 ±      0.558  MB/sec
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.churn.G1_Survivor_Space.norm     thrpt    6     17014.749 ±   3709.184    B/op
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.count                            thrpt    6       268.000               counts
WireProtocolWriterJMH.test20_serializeLegacyMappedLargeUnflattenObjects:·gc.time                             thrpt    6       339.000                   ms
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects                                       thrpt    6      1069.690 ±    500.873   ops/s
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.alloc.rate                        thrpt    6      2705.189 ±   1267.792  MB/sec
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.alloc.rate.norm                   thrpt    6   2917900.082 ±     36.824    B/op
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Eden_Space               thrpt    6       556.586 ±    274.411  MB/sec
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Eden_Space.norm          thrpt    6    599624.722 ±  20400.389    B/op
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Old_Gen                  thrpt    6      3864.494 ±   1829.134  MB/sec
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.churn.G1_Old_Gen.norm             thrpt    6   4167504.955 ± 101445.578    B/op
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.count                             thrpt    6       457.000               counts
WireProtocolWriterJMH.test21_serializeUnversionedLargeUnflattenObjects:·gc.time                              thrpt    6       404.000                   ms
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives                                         thrpt    6       300.446 ±     13.900   ops/s
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.alloc.rate                          thrpt    6      3559.099 ±    162.821  MB/sec
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.alloc.rate.norm                     thrpt    6  13669844.310 ±     36.859    B/op
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space                 thrpt    6      2967.800 ±    167.667  MB/sec
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm            thrpt    6  11398318.275 ± 209123.918    B/op
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen                    thrpt    6      1083.734 ±     61.295  MB/sec
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm               thrpt    6   4162255.985 ±  76695.624    B/op
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.count                               thrpt    6       354.000               counts
WireProtocolWriterJMH.test22_serializeMappedLargeUnflattenPrimitives:·gc.time                                thrpt    6       406.000                   ms
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives                                   thrpt    6       116.524 ±     25.395   ops/s
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.alloc.rate                    thrpt    6      1979.932 ±    428.411  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.alloc.rate.norm               thrpt    6  19607984.733 ±  36788.440    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space           thrpt    6      1747.432 ±    348.154  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm      thrpt    6  17314335.738 ± 750722.671    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen              thrpt    6       415.808 ±     84.385  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm         thrpt    6   4119679.490 ± 177293.061    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space       thrpt    6         0.968 ±      2.077  MB/sec
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.churn.G1_Survivor_Space.norm  thrpt    6     10040.065 ±  22516.877    B/op
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.count                         thrpt    6       237.000               counts
WireProtocolWriterJMH.test23_serializeLegacyMappedLargeUnflattenPrimitives:·gc.time                          thrpt    6       323.000                   ms
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives                                    thrpt    6      1017.943 ±    514.951   ops/s
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.alloc.rate                     thrpt    6      2573.858 ±   1301.086  MB/sec
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.alloc.rate.norm                thrpt    6   2917904.083 ±     24.613    B/op
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space            thrpt    6       519.974 ±    256.465  MB/sec
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Eden_Space.norm       thrpt    6    589874.598 ±  10550.377    B/op
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen               thrpt    6      3657.795 ±   1850.166  MB/sec
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.churn.G1_Old_Gen.norm          thrpt    6   4146642.241 ±  54589.804    B/op
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.count                          thrpt    6       501.000               counts
WireProtocolWriterJMH.test24_serializeUnversionedLargeUnflattenPrimitives:·gc.time                           thrpt    6       435.000                   ms
     */

    @Benchmark
    public void test01_serializeMappedSmallObjects(Serializers serializers, Blackhole blackhole) {
        blackhole.consume(serializers.smallObjects.serializeMappedObjects(serializers.smallObjectsData));
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
            Blackhole blackhole)
    {
        blackhole.consume(serializers.largeUnflattenPrimitives
                .serializeUnversionedObjects(serializers.largeUnflattenPrimitivesDataUnversioned));
    }

    @State(Scope.Thread)
    public static class Serializers extends ForClassInstantiationJMH.ObjectMetadata {
        private final List<SmallObjectClass> smallObjectsData;
        private final List<UnversionedRow> smallObjectsDataUnversioned;
        private final List<SmallPrimitiveClass> smallPrimitiveData;
        private final List<UnversionedRow> smallPrimitiveDataUnversioned;
        private final List<LargeObjectClass> largeObjectsData;
        private final List<UnversionedRow> largeObjectsDataUnversioned;
        private final List<LargePrimitiveClass> largePrimitiveData;
        private final List<UnversionedRow> largePrimitiveDataUnversioned;
        private final List<LargeFlattenObjectClass> largeFlattenObjectsData;
        private final List<UnversionedRow> largeFlattenObjectsDataUnversioned;
        private final List<LargeFlattenPrimitiveClass> largeFlattenPrimitivesData;
        private final List<UnversionedRow> largeFlattenPrimitivesDataUnversioned;
        private final List<LargeUnflattenObjectClass> largeUnflattenObjectsData;
        private final List<UnversionedRow> largeUnflattenObjectsDataUnversioned;
        private final List<LargeUnflattenPrimitiveClass> largeUnflattenPrimitivesData;
        private final List<UnversionedRow> largeUnflattenPrimitivesDataUnversioned;

        public Serializers() {
            smallObjectsData = smallObjects.generateObjects(1000);
            smallObjectsDataUnversioned = smallObjects.convertObjectsToUnversioned(smallObjectsData);
            smallPrimitiveData = smallPrimitives.generateObjects(1000);
            smallPrimitiveDataUnversioned = smallPrimitives.convertObjectsToUnversioned(smallPrimitiveData);
            largeObjectsData = largeObjects.generateObjects(1000);
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
            largeUnflattenObjectsDataUnversioned =
                    largeUnflattenObjects.convertObjectsToUnversioned(largeUnflattenObjectsData);
            largeUnflattenPrimitivesData = largeUnflattenPrimitives.generateObjects(1000);
            largeUnflattenPrimitivesDataUnversioned =
                    largeUnflattenPrimitives.convertObjectsToUnversioned(largeUnflattenPrimitivesData);
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WireProtocolWriterJMH.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(2)
                .build();

        new Runner(opt).run();
    }
}
