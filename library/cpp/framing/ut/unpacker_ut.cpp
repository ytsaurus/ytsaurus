#include <library/cpp/framing/packer.h>
#include <library/cpp/framing/unpacker.h>
#include <library/cpp/framing/syncword.h>

#include <util/string/builder.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/testing/gtest_protobuf/matcher.h>

#include <library/cpp/framing/ut/test.pb.h>


using namespace NFraming;
using namespace NFraming::NPrivate;
using namespace std::literals;

namespace {
    class TUnpackerTest : public testing::TestWithParam<EFormat> {};
}

TEST_P(TUnpackerTest, UnpackStringWithRecovery) {
    const TString source = [&] {
        switch (GetParam()) {
            case EFormat::Auto:
                [[fallthrough]];
            case EFormat::Protoseq:
                return TStringBuilder{}
                    << "\x09\x00\x00\x00"sv  // length
                    << "123456789"           // message
                    << SyncWord
                    << "corrupted"           // some corrupted message
                    << SyncWord
                    << "\x0b\x00\x00\x00"sv  // length
                    << "not checked"         // message (not checked protobuf-look-like)
                    << SyncWord
                    << "tail";               // some garbage to skip
                break;
            case EFormat::Lenval:
                return TStringBuilder{}
                    << "\t"          // length
                    << "123456789"   // message
                    << "\x0B"        // length2
                    << "not checked" // message2
                    << "tail";       // some garbage
                break;
            case EFormat::LightProtoseq:
                return TStringBuilder{}
                    << "\x09\x00\x00\x00"sv  // length
                    << "123456789"           // message
                    << "\x0b\x00\x00\x00"sv  // length
                    << "not checked"         // message (not checked protobuf-look-like)
                    << "tail";               // some garbage to skip
                break;
        }
    }();

    TUnpacker unpacker = [&] {
        if (EFormat::Auto == GetParam()) {
            return TUnpacker(source);
        }
        return TUnpacker(GetParam(), source);
    }();

    TStringBuf frame;
    TStringBuf skip;

    // expected 2 messages
    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ(frame, "123456789");
    EXPECT_EQ("", skip);

    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ(frame, "not checked");
    if (EFormat::Protoseq == GetParam()) {
        EXPECT_EQ(skip, TStringBuilder{} << "corrupted" << SyncWord);
    }
    EXPECT_FALSE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ("", frame);
    EXPECT_EQ(skip, "tail");

    EXPECT_FALSE(unpacker.NextFrame(frame, skip));
}

TEST_P(TUnpackerTest, UnpackProtobufWithRecovery) {
    NFramingTest::TB m1;
    m1.mutable_a()->set_i(1);
    m1.mutable_a()->set_s("1");
    m1.add_ff(3.0);
    m1.add_ff(4.0);

    NFramingTest::TB m2;
    m2.mutable_a()->set_i(2);
    m2.mutable_a()->set_s("2");
    m2.add_ff(5.0);

    const TString source = [&] {
        TString s;
        TStringOutput out(s);
        TPacker packer{GetParam(), out};
        packer.Add("some text1");
        packer.Add(m1);
        packer.Add(m2);
        packer.Add("some text3");
        packer.Flush();
        return s;
    }();

    auto skippedFromText = [&](const TString& text) -> TString {
        TString s;
        TStringOutput out(s);
        TPacker packer{GetParam(), out};
        packer.Add(text);
        packer.Flush();
        return s;
    };

    TUnpacker unpacker(GetParam(), source);

    TStringBuf skip;
    NFramingTest::TB frame;
    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ(TString(skip), skippedFromText("some text1"));
    EXPECT_THAT(frame, NGTest::EqualsProto(std::ref(m1)));
    EXPECT_TRUE(unpacker.NextFrame(frame, skip));

    EXPECT_THAT(frame, NGTest::EqualsProto(std::ref(m2)));
    EXPECT_TRUE(skip.empty());

    EXPECT_FALSE(unpacker.NextFrame(frame, skip));
    EXPECT_THAT(frame, NGTest::EqualsProto(NFramingTest::TB{}));
    EXPECT_EQ(TString(skip), skippedFromText("some text3"));
}

TEST_P(TUnpackerTest, UnpackEmptyData) {
    TStringBuf frame;
    TStringBuf skip;
    TUnpacker unpacker{GetParam(), TStringBuf{}};
    EXPECT_FALSE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ("", frame);
    EXPECT_EQ("", skip);
}

TEST_P(TUnpackerTest, UnpackStringAutoDetectFormat) {
    const TString source = PackToString(GetParam(), "hello") + PackToString(GetParam(), "world");
    TUnpacker unpacker(EFormat::Auto, source);
    TStringBuf frame;
    TStringBuf skip;

    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ(frame, "hello");
    EXPECT_EQ("", skip);
    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_EQ(frame, "world");
    EXPECT_EQ("", skip);
}

TEST_P(TUnpackerTest, UnpackProtobufAutoDetectFormat) {
    NFramingTest::TB m1;
    m1.mutable_a()->set_i(1);
    m1.mutable_a()->set_s("1");
    m1.add_ff(3.0);
    m1.add_ff(4.0);

    NFramingTest::TB m2;
    m2.mutable_a()->set_i(2);
    m2.mutable_a()->set_s("2");
    m2.add_ff(5.0);

    const TString source = PackToString(GetParam(), m1) + PackToString(GetParam(), m2);
    TUnpacker unpacker(EFormat::Auto, source);

    NFramingTest::TB frame;
    TStringBuf skip;

    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_THAT(frame, NGTest::EqualsProto(std::ref(m1)));
    EXPECT_EQ("", skip);
    EXPECT_TRUE(unpacker.NextFrame(frame, skip));
    EXPECT_THAT(frame, NGTest::EqualsProto(std::ref(m2)));
    EXPECT_EQ("", skip);
}

INSTANTIATE_TEST_SUITE_P(framing, TUnpackerTest, testing::Values(EFormat::Auto, EFormat::Protoseq, EFormat::Lenval));
