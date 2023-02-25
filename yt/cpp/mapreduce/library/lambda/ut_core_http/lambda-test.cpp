#include <yt/cpp/mapreduce/library/lambda/yt_lambda.h>
#include <yt/cpp/mapreduce/library/lambda/ut_core_http/dispersion.pb.h>

#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/utmain.h> // UNITTEST_WITH_CUSTOM_ENTRY_POINT

using namespace NYT;
using namespace NYT::NTesting;

static void CreateTable(IClientPtr client, const TRichYPath& tableName, const TVector<TNode>& table) {
    auto writer = client->CreateTableWriter<TNode>(tableName);
    for (auto& elem : table) {
        writer->AddRow(elem);
    }
    writer->Finish();
}

static void CompareTable(IClientPtr client, TString tableName, const TVector<TNode>& expected) {
    auto reader = client->CreateTableReader<TNode>(tableName);
    TVector<TNode> table;
    for (; reader->IsValid(); reader->Next()) {
        table.push_back(reader->GetRow());
    }
    UNIT_ASSERT_VALUES_EQUAL(expected, table);
}

const TVector<TNode> InputTableData = {
    TNode()("Key", "first")("Val", 1u),
    TNode()("Key", "second")("Val", 20u),
    TNode()("Key", "third")("Val", 300u),
    TNode()("Key", "first")("Val", 4000u),
};

const TVector<TNode> SortedInputTableData = {
    TNode()("key", "first")("val", 1u),
    TNode()("key", "first")("val", 4000u),
    TNode()("key", "second")("val", 20u),
    TNode()("key", "third")("val", 300u),
};

// Comparison with this works nicely only because (integer)/2.f is quite round
// in binary representation of float. Also note that sigma({N, 1}) == (N - 1)/2.
TVector<TNode> ExpectedOutputStatistics = {
    TNode()("key", "first")("mean", 2000.5)("sigma", 1999.5),
    TNode()("key", "second")("mean", 20.)("sigma", 0.),
    TNode()("key", "third")("mean", 300.)("sigma", 0.),
};

TVector<TNode> ExpectedOutputNF = {
    TNode()("key", "first")("count", 2u)("sum", 4001.)("sum_squared", 16000001.),
    TNode()("key", "second")("count", 1u)("sum", 20.)("sum_squared", 400.),
    TNode()("key", "third")("count", 1u)("sum", 300.)("sum_squared", 90000.),
};

struct TManuallyRegisteredSettings : ISerializableForJob {
    TString Stuff = "Bar";
    Y_SAVELOAD_JOB(Stuff);
};

TManuallyRegisteredSettings ManuallyRegisteredSettings;

struct TSomeUselessSettings {
    TString Foo = "Bar";
    Y_SAVELOAD_DEFINE(Foo);
};

TSaveable<TSomeUselessSettings> UselessSettings;

struct TSomeGlobalSettings {
    ui64 Limit = 0;
};

TSaveablePOD<TSomeGlobalSettings> GlobalSettings;

Y_UNIT_TEST_SUITE(Lambda) {
    Y_UNIT_TEST(CopyIf) {
        // Note that constants need not to be captured
        static constexpr ui64 LIMIT = 100;

        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        CopyIf<TNode>(client, "//testing/input",  "//testing/output",
            [](auto& row) { return row["Val"].AsUint64() < LIMIT; });

        TVector<TNode> expectedOutput = {
            TNode()("Key", "first")("Val", 1u),
            TNode()("Key", "second")("Val", 20u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(TransformCopyIf) {
        GlobalSettings.Limit = 1000;
        ManuallyRegisteredSettings.Stuff = "Stuff";
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        TransformCopyIf<TNode, TNode>(client, "//testing/input",  "//testing/output",
            [](auto& src, auto& dst) {
                if (src["Val"].AsUint64() >= GlobalSettings.Limit)
                    return false;
                dst["Key1"] = src["Key"];
                dst["Key2"] = src["Key"].AsString() + ManuallyRegisteredSettings.Stuff;
                dst["Val"] = src["Val"];
                return true;
            });

        TVector<TNode> expectedOutput = {
            TNode()("Key1", "first")("Key2", "firstStuff")("Val", 1u),
            TNode()("Key1", "second")("Key2", "secondStuff")("Val", 20u),
            TNode()("Key1", "third")("Key2", "thirdStuff")("Val", 300u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(AdditiveReduce) {
        auto client = CreateTestClient();
        CreateTable(client,
            TRichYPath("//testing/input").SortedBy("key"),
            SortedInputTableData);

        AdditiveReduce<TNode, TNode>(client, "//testing/input",  "//testing/output",
            { "key" },
            [](auto& src, auto& dst) { // reducer
                dst["val"] = dst["val"].AsUint64() + src["val"].AsUint64();
            },
            [](auto& src, auto& dst) { // finalizer
                dst["inval"] = -(i64)src["val"].AsUint64();
                return true;
            });

        TVector<TNode> expectedOutput = {
            TNode()("key", "first")("inval", -4001),
            TNode()("key", "second")("inval", -20),
            TNode()("key", "third")("inval", -300),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(AdditiveReduceNoFinalizer) {
        auto client = CreateTestClient();
        CreateTable(client,
            TRichYPath("//testing/input").SortedBy("key"),
            SortedInputTableData);

        AdditiveReduce<TNode>(client, "//testing/input",  "//testing/output",
            { "key" },
            [](auto& src, auto& dst) {
                dst["val"] = dst["val"].AsUint64() + src["val"].AsUint64();
            });

        TVector<TNode> expectedOutput = {
            TNode()("key", "first")("val", 4001u),
            TNode()("key", "second")("val", 20u),
            TNode()("key", "third")("val", 300u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    // * TDispersionDataMsg could be used instead of this structure,
    //   but look how clean the code is without Get/Set stuff.
    struct TDispersionData {
        ui64 Count = 0;
        long double Sum = 0.;
        long double SumSquared = 0.;
    };

    Y_UNIT_TEST(Reduce) {
        auto client = CreateTestClient();
        CreateTable(client,
            TRichYPath("//testing/input").SortedBy("key"),
            SortedInputTableData);

        Reduce<TNode, TDispersionData, TNode>(client, "//testing/input",  "//testing/output",
            { "key" },
            [](auto& src, auto& dst) { // reducer
                double value = src["val"].template ConvertTo<double>();
                dst.Count++;
                dst.Sum += value;
                dst.SumSquared += value * value;
            },
            [](auto& src, auto& dst) { // finalizer
                double mean = (double)src.Sum / src.Count;
                double dispersion = (double)src.SumSquared / src.Count - mean * mean;
                dst["mean"] = mean;
                dst["sigma"] = std::sqrt(dispersion);
                return true;
            });

        CompareTable(client, "//testing/output", ExpectedOutputStatistics);
    }

    Y_UNIT_TEST(ReduceNoFinalizer) {
        auto client = CreateTestClient();
        CreateTable(client,
            TRichYPath("//testing/input").SortedBy("key"),
            SortedInputTableData);

        /* Note that the use of TNode here is inconvenient.
           See MapReduceSortedNoFinalizer for similar protobuf usage.*/

        Reduce<TNode, TNode>(client, "//testing/input",  "//testing/output",
            { "key" },
            [](auto& src, auto& dst) { // reducer
                double value = src["val"].template ConvertTo<double>();
                dst["count"] = (dst.HasKey("count") ? dst["count"].AsUint64() : 0u) + 1;
                dst["sum"] = (dst.HasKey("sum") ? dst["sum"].AsDouble() : 0.) + value;
                dst["sum_squared"] =
                    (dst.HasKey("sum_squared") ? dst["sum_squared"].AsDouble() : 0.) + value * value;
            });

        CompareTable(client, "//testing/output", ExpectedOutputNF);
    }

    Y_UNIT_TEST(AdditiveMapReduceSorted) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        AdditiveMapReduceSorted<TNode, TNode, TNode>(client, "//testing/input",  "//testing/output",
            { "Key1", "Key2" },
            [](auto& src, auto& dst) { // mapper
                dst["Key1"] = src["Key"];
                dst["Key2"] = TString(src["Key"].AsString().back()) + src["Key"].AsString();
                dst["Val"] = src["Val"];
                return true;
            },
            [](auto& src, auto& dst) { // reducer
                dst["Val"] = dst["Val"].AsUint64() + src["Val"].AsUint64();
            },
            [](auto& src, auto& dst) { // finalizer
                dst["Val"] = -(i64)src["Val"].AsUint64();
                return true;
            });

        TVector<TNode> expectedOutput = {
            TNode()("Key1", "first")("Key2", "tfirst")("Val", -4001),
            TNode()("Key1", "second")("Key2", "dsecond")("Val", -20),
            TNode()("Key1", "third")("Key2", "dthird")("Val", -300),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(AdditiveMapReduceSortedNoFinalizer) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        AdditiveMapReduceSorted<TNode, TNode>(client, "//testing/input",  "//testing/output",
            { "Key1", "Key2" },
            [](auto& src, auto& dst) { // mapper
                dst["Key1"] = src["Key"];
                dst["Key2"] = TString(src["Key"].AsString().back()) + src["Key"].AsString();
                dst["Val"] = src["Val"];
                return true;
            },
            [](auto& src, auto& dst) { // reducer
                dst["Val"] = dst["Val"].AsUint64() + src["Val"].AsUint64();
            });

        TVector<TNode> expectedOutput = {
            TNode()("Key1", "first")("Key2", "tfirst")("Val", 4001u),
            TNode()("Key1", "second")("Key2", "dsecond")("Val", 20u),
            TNode()("Key1", "third")("Key2", "dthird")("Val", 300u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }

    Y_UNIT_TEST(MapReduceSorted) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        MapReduceSorted<TNode, TSimpleKeyValue, TDispersionData, TKeyStat>(
            client,
            "//testing/input",  "//testing/output",
            "key",
            [](auto& src, auto& dst) { // mapper
                dst.SetKey(src["Key"].AsString());
                dst.SetValue(src["Val"].AsUint64());
                return true;
            },
            [](auto& src, auto& dst) { // reducer
                double value = src.GetValue();
                dst.Count++;
                dst.Sum += value;
                dst.SumSquared += value * value;
            },
            [](auto& src, auto& dst) { // finalizer
                double mean = (double)src.Sum / src.Count;
                double dispersion = (double)src.SumSquared / src.Count - mean * mean;
                dst.SetMean(mean);
                dst.SetSigma(std::sqrt(dispersion));
                return true;
            });

        CompareTable(client, "//testing/output", ExpectedOutputStatistics);
    }

    Y_UNIT_TEST(MapReduceCombinedSorted) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        MapReduceCombinedSorted<TNode, TSimpleKeyValue, TDispersionDataMsg, TKeyStat>(
            client,
            "//testing/input",  "//testing/output",
            "key",
            [](auto& src, auto& dst) { // mapper
                dst.SetKey(src["Key"].AsString());
                dst.SetValue(src["Val"].AsUint64());
                return true;
            },
            [](auto& src, auto& dst) { // combiner
                double value = src.GetValue();
                dst.SetCount(dst.GetCount() + 1);
                dst.SetSum(dst.GetSum() + value);
                dst.SetSumSquared(dst.GetSumSquared() + value * value);
            },
            [](auto& src, auto& dst) { // reducer
                dst.SetCount(src.GetCount() + dst.GetCount());
                dst.SetSum(src.GetSum() + dst.GetSum());
                dst.SetSumSquared(src.GetSumSquared() + dst.GetSumSquared());
            },
            [](auto& src, auto& dst) { // finalizer
                double mean = src.GetSum() / src.GetCount();
                double dispersion = src.GetSumSquared() / src.GetCount() - mean * mean;
                dst.SetMean(mean);
                dst.SetSigma(std::sqrt(dispersion));
                return true;
            });

        CompareTable(client, "//testing/output", ExpectedOutputStatistics);
    }

    Y_UNIT_TEST(MapReduceSortedNoFinalizer) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        MapReduceSorted<TNode, TSimpleKeyValue, TDispersionDataMsg>(
            client,
            "//testing/input",  "//testing/output",
            "key",
            [](auto& src, auto& dst) { // mapper
                dst.SetKey(src["Key"].AsString());
                dst.SetValue(src["Val"].AsUint64());
                return true;
            },
            [](auto& src, auto& dst) { // reducer
                double value = src.GetValue();
                dst.SetCount(dst.GetCount() + 1);
                dst.SetSum(dst.GetSum() + value);
                dst.SetSumSquared(dst.GetSumSquared() + value * value);
            });

        CompareTable(client, "//testing/output", ExpectedOutputNF);
    }

    Y_UNIT_TEST(MapReduceCombinedSortedNoFinalizer) {
        auto client = CreateTestClient();
        CreateTable(client, "//testing/input", InputTableData);

        MapReduceCombinedSorted<TNode, TSimpleKeyValue, TDispersionDataMsg>(
            client,
            "//testing/input",  "//testing/output",
            "key",
            [](auto& src, auto& dst) { // mapper
                dst.SetKey(src["Key"].AsString());
                dst.SetValue(src["Val"].AsUint64());
                return true;
            },
            [](auto& src, auto& dst) { // combiner
                double value = src.GetValue();
                dst.SetCount(dst.GetCount() + 1);
                dst.SetSum(dst.GetSum() + value);
                dst.SetSumSquared(dst.GetSumSquared() + value * value);
            },
            [](auto& src, auto& dst) { // reducer
                dst.SetCount(src.GetCount() + dst.GetCount());
                dst.SetSum(src.GetSum() + dst.GetSum());
                dst.SetSumSquared(src.GetSumSquared() + dst.GetSumSquared());
            });

        CompareTable(client, "//testing/output", ExpectedOutputNF);
    }

    Y_UNIT_TEST(SortSep) {
        auto reduceSpec = NYT::NDetail::PrepareReduceSpec<TNode, TNode>(
            "//table1", "//table2", { "foo", SortBySep, "bar" });
        auto mrSpec = NYT::NDetail::PrepareMRSpec<TNode, TNode>(
            "//table1", "//table2", { "foo", SortBySep, "bar" });
        TSortColumns expectedReduceBy{ "foo" };
        TSortColumns expectedSortBy{ "foo", "bar" };
        UNIT_ASSERT_VALUES_EQUAL(reduceSpec.ReduceBy_.Parts_, expectedReduceBy.Parts_);
        UNIT_ASSERT_VALUES_EQUAL(reduceSpec.SortBy_.Parts_, expectedSortBy.Parts_);
        UNIT_ASSERT_VALUES_EQUAL(mrSpec.ReduceBy_.Parts_, expectedReduceBy.Parts_);
        UNIT_ASSERT_VALUES_EQUAL(mrSpec.SortBy_.Parts_,  expectedSortBy.Parts_);

        auto client = CreateTestClient();
        CreateTable(client,
            TRichYPath("//testing/input").SortedBy({"key", "val"}),
            SortedInputTableData);

        Reduce<TSimpleKeyValue, TSimpleKeyValue>(client,
            TRichYPath("//testing/input").RenameColumns({{"val", "value"}}),
            "//testing/output",
            { "key", SortBySep, "value" },
            [](auto& src, auto& dst) { // reducer
                if (src.GetValue() >= dst.GetValue()) {
                    dst.SetValue(src.GetValue());
                } else {
                    dst.SetValue(Max<i64>());
                }
            });

        TVector<TNode> expectedOutput = {
            TNode()("key", "first")("value", 4000u),
            TNode()("key", "second")("value", 20u),
            TNode()("key", "third")("value", 300u),
        };

        CompareTable(client, "//testing/output", expectedOutput);
    }
}

int main(int argc, const char** argv)
{
    NYT::TConfig::Get()->LogLevel = "debug";
    RegisterGlobalSaveable(ManuallyRegisteredSettings);
    NYT::Initialize(argc, argv, NYT::TInitializeOptions().CleanupOnTermination(true));
    return NUnitTest::RunMain(argc, const_cast<char**>(argv));
}
