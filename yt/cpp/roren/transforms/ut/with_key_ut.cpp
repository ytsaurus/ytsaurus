#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/transforms/with_key.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NRoren;

TEST(TWithKeyTest, Simple)
{
    auto pipeline = MakeLocalPipeline();
    std::vector<TKV<int, TString>> actualResult;

    pipeline | VectorRead(std::vector<TString>{
        "-1",
        "4",
        "42",
        "100500",
    }) | WithKey([] (const TString& input) {
        return FromString<int>(input);
    }) | VectorWrite(&actualResult);

    pipeline.Run();

    const auto expected = std::vector<TKV<int, TString>>{
        {-1, "-1"},
        {4, "4"},
        {42, "42"},
        {100500, "100500"},
    };

    ASSERT_EQ(expected, actualResult);
}
