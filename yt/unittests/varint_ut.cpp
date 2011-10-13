#include "../ytlib/misc/serialize.h"

#include <util/random/random.h>

#include <contrib/testing/framework.h>

namespace NYT {

using ::std::tr1::tuple;
using ::std::tr1::get;
using ::std::tr1::make_tuple;

using ::testing::Values;

////////////////////////////////////////////////////////////////////////////////

class TWriteVarIntTest: public ::testing::TestWithParam< tuple<ui64, Stroka> >
{ };

TEST_P(TWriteVarIntTest, Serialization)
{
    ui64 value = get<0>(GetParam());
    Stroka rightAnswer = get<1>(GetParam());

    TStringStream outputStream;
    WriteVarInt(value, &outputStream);
    EXPECT_EQ(rightAnswer, outputStream.Str());
}

INSTANTIATE_TEST_CASE_P(ValueParametrized, TWriteVarIntTest, Values(
    make_tuple(0x0ull,                  Stroka("\x00", 1)),
    make_tuple(0x1ull,                  Stroka("\x01", 1)),
    make_tuple(0x2ull,                  Stroka("\x02", 1)),
    make_tuple(0x3ull,                  Stroka("\x03", 1)),
    make_tuple(0x10ull,                 Stroka("\x10", 1)),
    make_tuple(0x1000ull,               Stroka("\x80\x20", 2)),
    make_tuple(0x100000ull,             Stroka("\x80\x80\x40", 3)),
    make_tuple(0x10000000ull,           Stroka("\x80\x80\x80\x80\x01", 5)),
    make_tuple(0x1000000000ull,         Stroka("\x80\x80\x80\x80\x80\x02", 6)),
    make_tuple(0x100000000000ull,       Stroka("\x80\x80\x80\x80\x80\x80\x04", 7)),
    make_tuple(0x10000000000000ull,     Stroka("\x80\x80\x80\x80\x80\x80\x80\x08", 8)),
    make_tuple(0x1000000000000000ull,   Stroka("\x80\x80\x80\x80\x80\x80\x80\x80\x10", 9))
));

////////////////////////////////////////////////////////////////////////////////

class TReadVarIntTest: public ::testing::TestWithParam< tuple<ui64, Stroka> >
{ };

TEST_P(TReadVarIntTest, Serialization)
{
    ui64 rightAnswer = get<0>(GetParam());
    Stroka input = get<1>(GetParam());

    TMemoryInput inputStream(input.c_str(), input.length());
    ui64 value = ReadVarInt(&inputStream);
    EXPECT_EQ(rightAnswer, value);
}

INSTANTIATE_TEST_CASE_P(ValueParametrized, TReadVarIntTest, Values(
    make_tuple(0x0ull,                  Stroka("\x00", 1)),
    make_tuple(0x1ull,                  Stroka("\x01", 1)),
    make_tuple(0x2ull,                  Stroka("\x02", 1)),
    make_tuple(0x3ull,                  Stroka("\x03", 1)),
    make_tuple(0x10ull,                 Stroka("\x10", 1)),
    make_tuple(0x1000ull,               Stroka("\x80\x20", 2)),
    make_tuple(0x100000ull,             Stroka("\x80\x80\x40", 3)),
    make_tuple(0x10000000ull,           Stroka("\x80\x80\x80\x80\x01", 5)),
    make_tuple(0x1000000000ull,         Stroka("\x80\x80\x80\x80\x80\x02", 6)),
    make_tuple(0x100000000000ull,       Stroka("\x80\x80\x80\x80\x80\x80\x04", 7)),
    make_tuple(0x10000000000000ull,     Stroka("\x80\x80\x80\x80\x80\x80\x80\x08", 8)),
    make_tuple(0x1000000000000000ull,   Stroka("\x80\x80\x80\x80\x80\x80\x80\x80\x10", 9))
));

////////////////////////////////////////////////////////////////////////////////

TEST(TVarInt32Test, SomeValues)
{
    srand(100500); // set seed
    int numValues = 10000;

    TStringStream stream;
    for (int i = 0; i < numValues; ++i) {
        i32 value = static_cast<i32>(RandomNumber<ui32>());
        WriteVarInt32(value, &stream);
        i32 readValue = ReadVarInt32(&stream);
        EXPECT_EQ(value, readValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TVarInt64Test, SomeValues)
{
    srand(100500); // set seed
    int numValues = 10000;

    TStringStream stream;
    for (int i = 0; i < numValues; ++i) {
        i64 value = static_cast<i64>(RandomNumber<ui64>());
        WriteVarInt64(value, &stream);
        i64 readValue = ReadVarInt64(&stream);
        EXPECT_EQ(value, readValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
