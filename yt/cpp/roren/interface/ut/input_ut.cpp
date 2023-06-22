#include <yt/cpp/roren/interface/input.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TEST(TTestMultiInput, Simple)
{
    auto tag1 = TTypeTag<int>{"t1"};
    auto tag2 = TTypeTag<TString>{"t2"};
    auto tag3 = TTypeTag<int>{"t3"};

    auto multiInput = NPrivate::CreateMultiInput({
        {
            tag1,
            MakeRawVectorInput<int>({1, 2, 3, 4})
        },
        {
            tag2,
            MakeRawVectorInput<TString>({"foo", "bar"})
        },
        {
            tag3,
            MakeRawVectorInput<int>({5, 6})
        }
    });

    {
        const auto expected = std::vector<int>{1, 2, 3, 4};
        const auto actual = ReadAllRows(multiInput->GetInput(tag1));
        EXPECT_EQ(expected, actual);
    }

    {
        const auto expected = std::vector<TString>{"foo", "bar"};
        const auto actual = ReadAllRows(multiInput->GetInput(tag2));
        EXPECT_EQ(expected, actual);
    }

    {
        const auto expected = std::vector<int>{5, 6};
        const auto actual = ReadAllRows(multiInput->GetInput(tag3));
        EXPECT_EQ(expected, actual);
    }
}

TEST(TTestMultiInput, CoderEmpty)
{
    auto multiInputOriginal = NPrivate::CreateMultiInput({});

    TStringStream ss;
    TCoder<TMultiInputPtr> coder;
    coder.Encode(&ss, multiInputOriginal);

    TMultiInputPtr multiInput;
    coder.Decode(&ss, multiInput);

    const auto expected = std::vector<TString>{};
    const auto actual = multiInput->GetInputDescriptionList();
    EXPECT_EQ(expected, actual);
}

TEST(TTestMultiInput, Coder)
{
    auto tag1 = TTypeTag<int>{"t1"};
    auto tag2 = TTypeTag<TString>{"t2"};
    auto tag3 = TTypeTag<int>{"t3"};

    auto multiInputOriginal = NPrivate::CreateMultiInput({
        {
            tag1,
            MakeRawVectorInput<int>({1, 2, 3, 4})
        },
        {
            tag2,
            MakeRawVectorInput<TString>({"foo", "bar"})
        },
        {
            tag3,
            MakeRawVectorInput<int>({5, 6})
        }
    });

    TStringStream ss;
    TCoder<TMultiInputPtr> coder;
    coder.Encode(&ss, multiInputOriginal);

    TMultiInputPtr multiInput;
    coder.Decode(&ss, multiInput);

    {
        const auto expected = std::vector<int>{1, 2, 3, 4};
        const auto actual = ReadAllRows(multiInput->GetInput(tag1));
        EXPECT_EQ(expected, actual);
    }

    {
        const auto expected = std::vector<TString>{"foo", "bar"};
        const auto actual = ReadAllRows(multiInput->GetInput(tag2));
        EXPECT_EQ(expected, actual);
    }

    {
        const auto expected = std::vector<int>{5, 6};
        const auto actual = ReadAllRows(multiInput->GetInput(tag3));
        EXPECT_EQ(expected, actual);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
