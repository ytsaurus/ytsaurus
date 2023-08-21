
#include <yt/cpp/roren/interface/coder.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

TEST(TTestCoder, Variant)
{
    using TVariant = std::variant<std::monostate, std::string, std::string, int>;
    TVariant before;
    before.emplace<1>("foo");


    TCoder<TVariant> coder;
    TString serialized;

    {
        TStringOutput out{serialized};
        coder.Encode(&out, before);
        out.Finish();
    }

    TVariant after;
    {
        TStringInput in{serialized};
        coder.Decode(&in, after);
    }
    ASSERT_EQ(before, after);
}

TEST(TTestCoder, Tuple)
{
    using TTuple = std::tuple<std::string, std::string, int>;
    TTuple before = {"foo", "bar", -42};

    TCoder<TTuple> coder;
    TString serialized;

    {
        TStringOutput out{serialized};
        coder.Encode(&out, before);
        out.Finish();
    }

    TTuple after;
    {
        TStringInput in{serialized};
        coder.Decode(&in, after);
    }
    ASSERT_EQ(before, after);


}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
