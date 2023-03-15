#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;

TEST(Lambda, SaveLoad)
{
    const std::vector<TString> input = {
        "foo",
        "bar",
        "baz",
    };

    std::vector<TString> output;

    auto pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | VectorWrite(&output);

    pipeline.Run();

    EXPECT_EQ(input, output);
}

TEST(Lambda, SaveLoadLambda)
{
    const std::vector<TString> input = {
        "foo",
        "bar",
        "baz",
    };

    std::vector<TString> output;

    TPipeline pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | ParDo([] (const TString& in) {
            return in + "!";
        })
        | VectorWrite(&output);

    pipeline.Run();

    const std::vector<TString> expected = {
        "foo!",
        "bar!",
        "baz!",
    };

    EXPECT_EQ(expected, output);
}

TEST(Lambda, SaveLoadLambda2)
{
    const std::vector<TString> input = {
        "foo",
        "bar",
        "baz",
    };

    std::vector<TString> output;

    TPipeline pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | ParDo([] (const TString& in, TOutput<TString>& output) {
            output.Add(in);
            output.Add(in);
        })
        | VectorWrite(&output);

    pipeline.Run();

    const std::vector<TString> expected = {
        "foo",
        "foo",
        "bar",
        "bar",
        "baz",
        "baz",
    };

    EXPECT_EQ(expected, output);
}

TEST(Lambda, GroupBy)
{
    const std::vector<TKV<TString, int>> input = {
        {"foo", 1},
        {"bar", 2},
        {"baz", 3},
        {"foo", 4},
        {"baz", 5},
    };

    std::vector<TKV<TString, std::vector<int>>> output;

    TPipeline pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | GroupByKey()
        | ParDo([] (const TKV<TString, TInputPtr<int>>& kv) {
            TKV<TString, std::vector<int>> result;
            result.Key() = kv.Key();

            for (const auto& v : kv.Value()) {
                result.Value().push_back(v);
            }
            std::sort(result.Value().begin(), result.Value().end());

            return result;
        })
        | VectorWrite(&output);

    pipeline.Run();

    std::sort(output.begin(), output.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Key() < rhs.Key();
    });

    const std::vector<std::pair<TString, std::vector<int>>> ee2 = {
        {"bar", {2}},
        {"baz", {3, 5}},
        {"foo", {1, 4}},
    };
    Y_UNUSED(ee2);

    const std::vector<TKV<TString, std::vector<int>>> expected = {
        {"bar", {2}},
        {"baz", {3, 5}},
        {"foo", {1, 4}},
    };

    EXPECT_EQ(expected, output);
}

TEST(Lambda, CombinePerKey)
{
    const std::vector<TKV<TString, int>> input = {
        {"foo", 1},
        {"bar", 2},
        {"baz", 3},
        {"foo", 4},
        {"baz", 5},
    };

    std::vector<TKV<TString, int>> output;

    TPipeline pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | CombinePerKey(Sum<int>())
        | VectorWrite(&output);
    pipeline.Run();

    std::sort(output.begin(), output.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Key() < rhs.Key();
    });

    const std::vector<TKV<TString, int>> expected = {
        {"bar", 2},
        {"baz", 8},
        {"foo", 5},
    };

    EXPECT_EQ(expected, output);
}
