#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <library/cpp/testing/gtest/gtest.h>


using namespace NRoren;


static const auto Input1Tag = TTypeTag<TKV<int, TString>>{"input1"};
static const auto Input2Tag = TTypeTag<TKV<int, TString>>{"input2"};

TEST(CoGroupByKey, Simple)
{
    auto pipeline = MakeLocalPipeline();
    auto input1 = pipeline |
        VectorRead(std::vector<TKV<int, TString>>{
            {1, "foo"},
            {1, "bar"},
            {3, "baz"},
            {4, "qux"},
        });

    auto input2 = pipeline | VectorRead(std::vector<TKV<int, TString>>{
        {1, "one1"},
        {1, "one2"},
        {2, "two1"},
        {3, "three1"},
        {3, "three2"},
        {5, "five1"},
    });

    auto multiPCollection = TMultiPCollection{Input1Tag, input1, Input2Tag, input2};

    auto output = multiPCollection | CoGroupByKey() | ParDo([] (const TCoGbkResult& gbk) {
        TKV<int, TString> result;

        result.Key() = gbk.GetKey<int>();

        for (const auto& v : gbk.GetInput(Input1Tag)) {
            result.Value() += v.Value();
            result.Value() += ' ';
        }

        for (const auto& v : gbk.GetInput(Input2Tag)) {
            result.Value() += v.Value();
            result.Value() += ' ';
        }

        Y_VERIFY(!result.Value().empty());
        result.Value().pop_back(); // strip trailing space

        return result;
    });

    std::vector<TKV<int, TString>> result;
    output | VectorWrite(& result);

    pipeline.Run();

    std::sort(result.begin(), result.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.Key() < rhs.Key();
    });

    const auto expected = std::vector<TKV<int, TString>>{
        {1, "foo bar one1 one2"},
        {2, "two1"},
        {3, "baz three1 three2"},
        {4, "qux"},
        {5, "five1"},
    };
    ASSERT_EQ(result, expected);
}

TEST(CoGroupByKey, Empty)
{
    auto pipeline = MakeLocalPipeline();

    auto multiPCollection = TMultiPCollection{pipeline};

    auto output = multiPCollection | CoGroupByKey() | ParDo([] (const TCoGbkResult& gbk) {
        return gbk.GetKey<int>();
    });

    std::vector<int> result;
    output | VectorWrite(&result);

    pipeline.Run();

    const auto expected = std::vector<int>{};
    ASSERT_EQ(result, expected);
}
