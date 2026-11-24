#include <library/cpp/framing/packer.h>
#include <library/cpp/framing/syncword.h>
#include <library/cpp/framing/unpacker.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/stream/mem.h>
#include <util/stream/str.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/testing/gtest_protobuf/matcher.h>

#include <library/cpp/framing/ut/test.pb.h>

using namespace NFraming;
using namespace NFraming::NPrivate;
using namespace std::literals;

namespace {
    class TPackerTest : public testing::TestWithParam<EFormat> {
    protected:
        TPacker CreatePacker(IOutputStream& out) const {
            if (EFormat::Auto == GetParam()) {
                return TPacker(out);
            }
            return TPacker(GetParam(), out);
        }
    };
}

TEST_P(TPackerTest, PackStrings) {
    const TString expected = [=]() {
        switch (GetParam()) {
            case EFormat::Auto:
            case EFormat::Protoseq:
                return TStringBuilder{}
                    << "\x09\x00\x00\x00"sv  // length
                    << "123456789"
                    << SyncWord
                    << "\x0b\x00\x00\x00"sv  // length
                    << "not checked"
                    << SyncWord;
                break;
            case EFormat::Lenval:
                return TStringBuilder{}
                    << "\t"            // length
                    << "123456789"
                    << "\x0B"          // length2
                    << "not checked";
                break;
            case EFormat::LightProtoseq:
                return TStringBuilder{}
                    << "\x09\x00\x00\x00"sv  // length
                    << "123456789"
                    << "\x0b\x00\x00\x00"sv  // length
                    << "not checked";
                break;
        }
    }();

    TStringStream out;
    TPacker packer = CreatePacker(out);

    packer.Add("123456789");
    EXPECT_GT(out.Size(), 9u);
    packer.Add("not checked");
    EXPECT_GT(out.Size(), 20u);
    packer.Flush();
    EXPECT_EQ(expected, out.Str());

    TStringStream out2;
    Pack(GetParam(), out2, TVector<TStringBuf>{"123456789", "not checked"});
    EXPECT_EQ(expected, out2.Str());

    EXPECT_EQ(expected, PackToString(GetParam(), "123456789") + PackToString(GetParam(), "not checked"));
}

TEST_P(TPackerTest, PackProtobufs) {
    NFramingTest::TB m0;
    m0.mutable_a()->set_i(1);
    m0.mutable_a()->set_s("2");
    m0.add_ff(3.0);
    m0.add_ff(4.0);

    TStringStream out;
    {
        TPacker packer{GetParam(), out};
        packer.Add(m0); // this call forces caching of size
        packer.Add(m0, true);
        packer.Flush();
    }

    {
        TString s;
        s += PackToString(GetParam(), m0);
        s += PackToString(GetParam(), m0, true);
        EXPECT_EQ(out.Str(), s);
    }

    {
        TUnpacker unpacker(GetParam(), out.Str());
        TStringBuf skip;
        NFramingTest::TB m1;

        EXPECT_TRUE(unpacker.NextFrame(m1, skip));
        EXPECT_THAT(m0, NGTest::EqualsProto(std::ref(m1)));
        m1.add_ff(3.0); // change something, to ensure that NextFrame fill message properly
        EXPECT_TRUE(unpacker.NextFrame(m1, skip));
        EXPECT_THAT(m0, NGTest::EqualsProto(std::ref(m1)));
        EXPECT_FALSE(unpacker.NextFrame(m1, skip));
    }
}

INSTANTIATE_TEST_SUITE_P(framing, TPackerTest, testing::Values(EFormat::Auto, EFormat::Protoseq, EFormat::Lenval));
