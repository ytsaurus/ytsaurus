#include <yt/cpp/roren/interface/co_gbk_result.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value, typename... Args>
void FillTypedInputs(
    std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>>* taggedInputList,
    const TTypeTag<TKV<Key, Value>>& tag,
    std::vector<TKV<Key, Value>> data,
    Args... args)
{
    taggedInputList->emplace_back(tag, MakeRawVectorInput(std::move(data)));
    if constexpr (sizeof...(args) > 0) {
        FillTypedInputs<Key>(taggedInputList, args...);
    }
}

template <typename Key, typename Value, typename... Args>
TCoGbkResult MakeCoGbkResult(
    Key key,
    const TTypeTag<TKV<Key, Value>>& tag,
    std::vector<TKV<Key, Value>> data,
    Args... args)
{
    std::vector<std::pair<TDynamicTypeTag, NPrivate::IRawInputPtr>> taggedInputList;
    FillTypedInputs(&taggedInputList, tag, data, args...);

    NPrivate::TRawRowHolder rowHolder(NPrivate::MakeRowVtable<Key>());
    *static_cast<Key*>(rowHolder.GetData()) = std::move(key);
    return MakeCoGbkResult(std::move(rowHolder), std::move(taggedInputList));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTestCoGbkResult, Simple)
{
    const auto tag1 = TTypeTag<TKV<TString, int>>{"t1"};
    const auto tag2 = TTypeTag<TKV<TString, TString>>{"t2"};
    const auto tag3 = TTypeTag<TKV<TString, int>>{"t3"};

    const auto expected1 = std::vector<TKV<TString, int>>{{"my-key", 1}, {"my-key", 2}, {"my-key", 3}};
    const auto expected2 = std::vector<TKV<TString, TString>>{{"my-key", "foo"}, {"my-key", "bar"}};
    const auto expected3 = std::vector<TKV<TString, int>>{{"my-key", 5}, {"my-key", 6}};

    auto gbk = MakeCoGbkResult(
        TString{"my-key"},
        tag1,
        expected1,
        tag2,
        expected2,
        tag3,
        expected3
    );

    EXPECT_EQ(gbk.GetKey<TString>(), "my-key");

    {
        const auto actual = ReadAllRows(gbk.GetInput(tag1));
        EXPECT_EQ(expected1, actual);
    }

    {
        const auto actual = ReadAllRows(gbk.GetInput(tag2));
        EXPECT_EQ(expected2, actual);
    }

    {
        const auto actual = ReadAllRows(gbk.GetInput(tag3));
        EXPECT_EQ(expected3, actual);
    }
}

TEST(TTestCoGbkResult, SaveLoad)
{
    const auto tag1 = TTypeTag<TKV<TString, int>>{"t1"};
    const auto tag2 = TTypeTag<TKV<TString, TString>>{"t2"};
    const auto tag3 = TTypeTag<TKV<TString, int>>{"t3"};

    const auto expected1 = std::vector<TKV<TString, int>>{{"my-key", 1}, {"my-key", 2}, {"my-key", 3}};
    const auto expected2 = std::vector<TKV<TString, TString>>{{"my-key", "foo"}, {"my-key", "bar"}};
    const auto expected3 = std::vector<TKV<TString, int>>{{"my-key", 5}, {"my-key", 6}};

    auto originalGbk = MakeCoGbkResult(
        TString{"my-key"},
        tag1,
        expected1,
        tag2,
        expected2,
        tag3,
        expected3
    );

    TStringStream ss;
    {
        TCoder<TCoGbkResult> coder;
        coder.Encode(&ss, originalGbk);
    }

    TCoGbkResult gbk;
    {
        TCoder<TCoGbkResult> coder;
        coder.Decode(&ss, gbk);
    }

    EXPECT_EQ(gbk.GetKey<TString>(), "my-key");

    {
        const auto actual = ReadAllRows(gbk.GetInput(tag1));
        EXPECT_EQ(expected1, actual);
    }

    {
        const auto actual = ReadAllRows(gbk.GetInput(tag2));
        EXPECT_EQ(expected2, actual);
    }

    {
        const auto actual = ReadAllRows(gbk.GetInput(tag3));
        EXPECT_EQ(expected3, actual);
    }
}

TEST(TTestCoGbkResult, SaveLoadEmpty)
{
    auto originalGbk = NPrivate::MakeCoGbkResult(NPrivate::TRawRowHolder{}, {});

    TStringStream ss;
    {
        TCoder<TCoGbkResult> coder;
        coder.Encode(&ss, originalGbk);
    }

    TCoGbkResult gbk;
    {
        TCoder<TCoGbkResult> coder;
        coder.Decode(&ss, gbk);
    }

    EXPECT_EQ(gbk.GetInputDescriptionList(), std::vector<TString>{});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
