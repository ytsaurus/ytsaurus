#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/varint.h>

#include <util/random/random.h>

#include <util/string/escape.h>

namespace NYT {
namespace {

using ::std::tr1::tuple;
using ::std::tr1::get;
using ::std::tr1::make_tuple;

using ::testing::Values;

////////////////////////////////////////////////////////////////////////////////

class TWriteVarIntTest: public ::testing::TestWithParam< tuple<ui64, TString> >
{ };

TEST_P(TWriteVarIntTest, Serialization)
{
    ui64 value = get<0>(GetParam());
    TString rightAnswer = get<1>(GetParam());

    TStringStream outputStream;
    WriteVarUint64(&outputStream, value);
    EXPECT_EQ(rightAnswer, outputStream.Str());
}

////////////////////////////////////////////////////////////////////////////////

class TReadVarIntTest: public ::testing::TestWithParam< tuple<ui64, TString> >
{ };

TEST_P(TReadVarIntTest, Serialization)
{
    ui64 rightAnswer = get<0>(GetParam());
    TString input = get<1>(GetParam());

    TStringInput inputStream(input);
    ui64 value;
    ReadVarUint64(&inputStream, &value);
    EXPECT_EQ(rightAnswer, value);
}

TEST(TReadVarIntTest, Overflow)
{
    TString input("\x80\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", 11);
    TStringInput inputStream(input);
    ui64 value;
    EXPECT_ANY_THROW(ReadVarUint64(&inputStream, &value));
}

////////////////////////////////////////////////////////////////////////////////

auto ValuesForVarIntTests = Values(
    // Simple cases.
    make_tuple(0x0ull,                TString("\x00", 1)),
    make_tuple(0x1ull,                TString("\x01", 1)),
    make_tuple(0x2ull,                TString("\x02", 1)),
    make_tuple(0x3ull,                TString("\x03", 1)),
    make_tuple(0x4ull,                TString("\x04", 1)),

    // The following "magic numbers" are critical points for varint encoding.
    make_tuple((1ull << 7) - 1,       TString("\x7f", 1)),
    make_tuple((1ull << 7),           TString("\x80\x01", 2)),
    make_tuple((1ull << 14) - 1,      TString("\xff\x7f", 2)),
    make_tuple((1ull << 14),          TString("\x80\x80\x01", 3)),
    make_tuple((1ull << 21) - 1,      TString("\xff\xff\x7f", 3)),
    make_tuple((1ull << 21),          TString("\x80\x80\x80\x01", 4)),
    make_tuple((1ull << 28) - 1,      TString("\xff\xff\xff\x7f", 4)),
    make_tuple((1ull << 28),          TString("\x80\x80\x80\x80\x01", 5)),
    make_tuple((1ull << 35) - 1,      TString("\xff\xff\xff\xff\x7f", 5)),
    make_tuple((1ull << 35),          TString("\x80\x80\x80\x80\x80\x01", 6)),
    make_tuple((1ull << 42) - 1,      TString("\xff\xff\xff\xff\xff\x7f", 6)),
    make_tuple((1ull << 42),          TString("\x80\x80\x80\x80\x80\x80\x01", 7)),
    make_tuple((1ull << 49) - 1,      TString("\xff\xff\xff\xff\xff\xff\x7f", 7)),
    make_tuple((1ull << 49),          TString("\x80\x80\x80\x80\x80\x80\x80\x01", 8)),
    make_tuple((1ull << 56) - 1,      TString("\xff\xff\xff\xff\xff\xff\xff\x7f", 8)),
    make_tuple((1ull << 56),          TString("\x80\x80\x80\x80\x80\x80\x80\x80\x01", 9)),
    make_tuple((1ull << 63) - 1,      TString("\xff\xff\xff\xff\xff\xff\xff\xff\x7f", 9)),
    make_tuple((1ull << 63),          TString("\x80\x80\x80\x80\x80\x80\x80\x80\x80\x01", 10)),

    // Boundary case.
    make_tuple(static_cast<ui64>(-1), TString("\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01", 10))
);

INSTANTIATE_TEST_CASE_P(ValueParametrized, TWriteVarIntTest,
    ValuesForVarIntTests);

INSTANTIATE_TEST_CASE_P(ValueParametrized, TReadVarIntTest,
    ValuesForVarIntTests);

////////////////////////////////////////////////////////////////////////////////

TEST(TVarInt32Test, RandomValues)
{
    srand(100500); // Set seed
    const int numberOfValues = 10000;

    TStringStream stream;
    for (int i = 0; i < numberOfValues; ++i) {
        i32 expected = static_cast<i32>(RandomNumber<ui32>());
        WriteVarInt32(&stream, expected);
        i32 actual;
        ReadVarInt32(&stream, &actual);
        EXPECT_EQ(expected, actual)
            << "Encoded Variant: " << EscapeC(stream.Str());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVarInt64Test, RandomValues)
{
    srand(100500); // Set seed
    const int numberOfValues = 10000;

    TStringStream stream;
    for (int i = 0; i < numberOfValues; ++i) {
        i64 expected = static_cast<i64>(RandomNumber<ui64>());
        WriteVarInt64(&stream, expected);
        i64 actual;
        ReadVarInt64(&stream, &actual);
        EXPECT_EQ(expected, actual)
            << "Encoded Variant: " << EscapeC(stream.Str());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
