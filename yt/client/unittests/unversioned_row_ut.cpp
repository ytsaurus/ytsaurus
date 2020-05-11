#include <yt/client/table_client/helpers.h>

#include <yt/core/test_framework/framework.h>

#include <yt/core/yson/token_writer.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/ypath_client.h>

#include <util/stream/str.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TUnversionedValue, TestConversionToYsonTokenWriter)
{
    auto convert = [] (TUnversionedValue value) {
        TStringStream stream;
        TCheckedInDebugYsonTokenWriter tokenWriter(&stream);
        UnversionedValueToYson(value, &tokenWriter);
        tokenWriter.Finish();
        return stream.Str();
    };

    {
        auto value = MakeUnversionedInt64Value(-42);
        i64 parsed = 0;
        EXPECT_NO_THROW(parsed = ConvertTo<i64>(TYsonString(convert(value))));
        EXPECT_EQ(parsed, -42);
    }
    {
        auto value = MakeUnversionedUint64Value(std::numeric_limits<ui64>::max());
        ui64 parsed = 0;
        EXPECT_NO_THROW(parsed = ConvertTo<ui64>(TYsonString(convert(value))));
        EXPECT_EQ(parsed, std::numeric_limits<ui64>::max());
    }
    {
        auto value = MakeUnversionedDoubleValue(2.718);
        double parsed = 0.0;
        EXPECT_NO_THROW(parsed = ConvertTo<double>(TYsonString(convert(value))));
        EXPECT_DOUBLE_EQ(parsed, 2.718);
    }
    {
        auto value = MakeUnversionedStringValue("boo");
        TString parsed;
        EXPECT_NO_THROW(parsed = ConvertTo<TString>(TYsonString(convert(value))));
        EXPECT_EQ(parsed, "boo");
    }
    {
        auto value = MakeUnversionedNullValue();
        TString str;
        EXPECT_NO_THROW(str = convert(value));
        EXPECT_EQ(str, "#");
    }
    {
        auto value = MakeUnversionedAnyValue("{x=y;z=<a=b>2}");
        INodePtr parsed;
        EXPECT_NO_THROW(parsed = ConvertTo<INodePtr>(TYsonString(convert(value))));
        auto expected = BuildYsonNodeFluently()
            .BeginMap()
                .Item("x").Value("y")
                .Item("z")
                    .BeginAttributes()
                        .Item("a").Value("b")
                    .EndAttributes()
                    .Value(2)
            .EndMap();
        EXPECT_TRUE(AreNodesEqual(parsed, expected))
            << "parsed: " << ConvertToYsonString(parsed, EYsonFormat::Pretty).GetData()
            << "\nexpected: " << ConvertToYsonString(expected, EYsonFormat::Pretty).GetData();
    }
}

////////////////////////////////////////////////////////////////////////////////
// Some sanity tests.
static_assert(TUnversionedValueConversionTraits<i64>::Inline, "i64 must be inline.");
static_assert(TUnversionedValueConversionTraits<i64>::Scalar, "i64 must be scalar.");
static_assert(TUnversionedValueConversionTraits<std::optional<i64>>::Inline, "i64? must be inline.");
static_assert(TUnversionedValueConversionTraits<std::optional<i64>>::Scalar, "i64? must be scalar.");
static_assert(!TUnversionedValueConversionTraits<TString>::Inline, "TString must not be inline.");
static_assert(TUnversionedValueConversionTraits<TString>::Scalar, "TString must be scalar.");

////////////////////////////////////////////////////////////////////////////////

TEST(TMakeUnversionedOwningRow, Empty)
{
    auto row = MakeUnversionedOwningRow();
    EXPECT_EQ(0, row.GetCount());
}

template <class T>
void CheckSingleValue(T value)
{
    auto row = MakeUnversionedOwningRow(value);
    EXPECT_EQ(1, row.GetCount());
    EXPECT_EQ(0, row[0].Id);
    EXPECT_EQ(value, FromUnversionedValue<T>(row[0]));
}

TEST(TMakeUnversionedOwningRow, SingleValue)
{
    CheckSingleValue(TGuid::Create());
    CheckSingleValue(TString("hello"));
    CheckSingleValue(AsStringBuf("hello"));
    CheckSingleValue(true);
    CheckSingleValue(TYsonString("{a=1}"));
    CheckSingleValue(static_cast<i64>(-123));
    CheckSingleValue(static_cast<ui64>(123));
    CheckSingleValue(static_cast<i32>(-17));
    CheckSingleValue(static_cast<ui32>(17));
    CheckSingleValue(static_cast<i16>(-2342));
    CheckSingleValue(static_cast<ui16>(2342));
    CheckSingleValue(static_cast<i8>(-12));
    CheckSingleValue(static_cast<ui8>(12));
    CheckSingleValue(static_cast<double>(3.14));
    CheckSingleValue(TInstant::Now());
}

TEST(TMakeUnversionedOwningRow, Tuple)
{
    auto row = MakeUnversionedOwningRow(TString("hello"), true);
    EXPECT_EQ(2, row.GetCount());
    EXPECT_EQ(0, row[0].Id);
    EXPECT_EQ("hello", FromUnversionedValue<TString>(row[0]));
    EXPECT_EQ(1, row[1].Id);
    EXPECT_EQ(true, FromUnversionedValue<bool>(row[1]));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
