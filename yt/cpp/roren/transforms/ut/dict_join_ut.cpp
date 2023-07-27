#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/dict_io.h>
#include <yt/cpp/roren/transforms/dict_join.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;

TEST(TTestDictJoin, Simple) {
    auto pipeline = MakeLocalPipeline();
    std::vector<TString> actualResult;

    auto pDict = pipeline | VectorRead(std::vector<TKV<TString, int>>{
        {"one", 1},
        {"two", 2},
        {"three", 3},
    });

    pipeline |
        VectorRead(std::vector<TKV<TString, TString>>{
            {"four", "_4444"},
            {"two", "_22"},
            {"one", "_1"},
        }) |
        DictJoin(pDict) |
        ParDo([](const std::tuple<TString, TString, std::optional<int>>& input) {
            auto& [key, value, dictValue] = input;
            TString result = key + ' ' + value + ' ';
            if (dictValue) {
                result += ToString(*dictValue);
            } else {
                result += "null";
            }
            return result;
        }) |
        VectorWrite(&actualResult);

    pipeline.Run();

    const auto expected = std::vector<TString>{
        "four _4444 null",
        "one _1 1",
        "two _22 2",
    };

    std::sort(actualResult.begin(), actualResult.end());

    ASSERT_EQ(expected, actualResult);
}

TEST(TTestDictJoin, ParDoCase) {
    auto pipeline = MakeLocalPipeline();
    std::vector<TString> actualResult;

    auto pDict = pipeline | DictRead(THashMap<TString, int>{
        {"one", 1},
        {"two", 2},
        {"three", 3},
    });

    pipeline |
        VectorRead(std::vector<TKV<TString, TString>>{
            {"four", "_4444"},
            {"two", "_22"},
            {"one", "_1"},
        }) |
        DictJoin(pDict) |
        ParDo([](const std::tuple<TString, TString, std::optional<int>>& input) {
            auto& [key, value, dictValue] = input;
            TString result = key + ' ' + value + ' ';
            if (dictValue) {
                result += ToString(*dictValue);
            } else {
                result += "null";
            }
            return result;
        }) |
        VectorWrite(&actualResult);

    pipeline.Run();

    const auto expected = std::vector<TString>{
        "four _4444 null",
        "one _1 1",
        "two _22 2",
    };

    std::sort(actualResult.begin(), actualResult.end());

    ASSERT_EQ(expected, actualResult);
}
