#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/local/local.h>
#include <yt/cpp/roren/local/vector_io.h>
#include <yt/cpp/roren/transforms/sum.h>

#include <library/cpp/testing/gtest/gtest.h>

#include "set_filter.h"

using namespace NRoren;

TEST(ParDo, ParDo)
{
    const std::vector<TString> input = {
        "foo",
        "bar",
        "baz",
        "qux"
    };

    std::vector<TString> output;

    TPipeline pipeline = MakeLocalPipeline();
    pipeline
        | VectorRead(input)
        | ParDo(::MakeIntrusive<TSetFilterParDo<TString>>(THashSet<TString>{"baz", "x", "foo"}))
        | VectorWrite(&output);

    pipeline.Run();

    const std::vector<TString> expected = {
        "foo",
        "baz",
        };

    ASSERT_EQ(expected, output);
}

namespace {

class TOddEvenSplitterParDo
    : public IDoFn<int, TMultiRow>
{
public:
    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {OddTag, EvenTag};
    }

    void Do(const int& x, TMultiOutput& output) override
    {
        if (x % 2 == 0) {
            output.GetOutput(EvenTag).Add(ToString(x));
        } else {
            output.GetOutput(OddTag).Add(x);
        }
    }

public:
    static const TTypeTag<int> OddTag;
    static const TTypeTag<TString> EvenTag;
};

const TTypeTag<int> TOddEvenSplitterParDo::OddTag = TTypeTag<int>{"odd"};
const TTypeTag<TString> TOddEvenSplitterParDo::EvenTag = TTypeTag<TString>{"even"};

} // namespace // anonymous

TEST(ParDo, ParDoMultipleOutputsClass)
{
    const std::vector<int> input = {1, 2, 5, 3, 8, 67, 66, 85};

    TPipeline pipeline = MakeLocalPipeline();

    auto splited = pipeline
            | VectorRead(input)
            | MakeParDo<TOddEvenSplitterParDo>();
    const auto& [odd, even] = splited.Unpack(TOddEvenSplitterParDo::OddTag, TOddEvenSplitterParDo::EvenTag);

    std::vector<int> oddVector;
    std::vector<TString> evenVector;
    odd | VectorWrite(&oddVector);
    even | VectorWrite(&evenVector);

    pipeline.Run();

    ASSERT_EQ(
        std::vector<TString>({"2", "8", "66"}),
        evenVector);
    ASSERT_EQ(
        std::vector<int>({1, 5, 3, 67, 85}),
        oddVector);
}

std::vector<int> GlobalVector;

class TWriteGlobalVector
    : public IDoFn<int, void>
{
public:
    void Do(const int& x, TOutput<void>&) override
    {
        GlobalVector.push_back(x);
    }
};

class TParDoVoidOutput : public ::testing::TestWithParam<TTransform<int, void>>
{};

INSTANTIATE_TEST_SUITE_P(Writers, TParDoVoidOutput, ::testing::Values(
    MakeParDo<TWriteGlobalVector>(),
    ParDo([] (const int& x) {
        GlobalVector.push_back(x);
    }),
    ParDo([] (const int& x, TOutput<void>&) {
        GlobalVector.push_back(x);
    })
));

TEST_P(TParDoVoidOutput, Test)
{
    TPipeline pipeline = MakeLocalPipeline();

    GlobalVector.clear();

    auto writeTransform = GetParam();

    pipeline | VectorRead(std::vector{1, 2, 3, 4}) | ParDo([] (const int& x) {
        return x * 2;
    }) | writeTransform;

    pipeline.Run();

    auto expected = std::vector{2, 4, 6, 8};
    EXPECT_EQ(GlobalVector, expected);
}
