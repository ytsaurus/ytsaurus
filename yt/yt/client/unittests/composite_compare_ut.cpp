#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/composite_compare.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/writer.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TCompositeCompare, Simple)
{
    EXPECT_EQ(-1, CompareCompositeValues("-4", "42"));
    EXPECT_EQ(1, CompareCompositeValues("42", "-4"));
    EXPECT_EQ(0, CompareCompositeValues("-4", "-4"));

    EXPECT_EQ(-1, CompareCompositeValues("4u", "42u"));
    EXPECT_EQ(1, CompareCompositeValues("42u", "4u"));
    EXPECT_EQ(0, CompareCompositeValues("4u", "4u"));

    EXPECT_EQ(-1, CompareCompositeValues("4.0", "4.2"));
    EXPECT_EQ(1, CompareCompositeValues("4.2", "4.0"));
    EXPECT_EQ(0, CompareCompositeValues("4.0", "4.0"));

    EXPECT_EQ(1, CompareCompositeValues("%nan ", "-5.0"));
    EXPECT_EQ(-1, CompareCompositeValues("-5.0", "%nan "));
    EXPECT_EQ(0, CompareCompositeValues("%nan ", "%nan "));

    EXPECT_EQ(-1, CompareCompositeValues("%false", "%true"));
    EXPECT_EQ(1, CompareCompositeValues("%true", "%false"));
    EXPECT_EQ(0, CompareCompositeValues("%true", "%true"));

    EXPECT_EQ(1, CompareCompositeValues("foo", "bar"));
    EXPECT_EQ(-1, CompareCompositeValues("foo", "fooo"));
    EXPECT_EQ(0, CompareCompositeValues("foo", "foo"));

    // Tuple<Int64,Int64> or List<Int64>
    EXPECT_EQ(-1, CompareCompositeValues("[1; 2]", "[1; 3]"));
    EXPECT_EQ(1, CompareCompositeValues("[1; 3]", "[1; 2]"));

    // List<Int64>
    EXPECT_EQ(1, CompareCompositeValues("[1; 2; 3]", "[1; 2]"));
    EXPECT_EQ(-1, CompareCompositeValues("[1; 2]", "[1; 2; 3]"));
    EXPECT_EQ(0, CompareCompositeValues("[1; 2; 3]", "[1; 2; 3]"));

    // List<Optional<Int64>>
    EXPECT_EQ(1, CompareCompositeValues("[1; 2; #]", "[1; 2]"));
    EXPECT_EQ(-1, CompareCompositeValues("[1; 2]", "[1; 2; #]"));
    EXPECT_EQ(-1, CompareCompositeValues("[1; 2; #]", "[1; 2; 3]"));
    EXPECT_EQ(1, CompareCompositeValues("[1; 2; 3]", "[1; 2; #]"));
}

TEST(TCompositeCompare, CompositeFingerprint)
{
    EXPECT_EQ(CompositeHash("-42"), GetFarmFingerprint(MakeUnversionedInt64Value(-42)));

    EXPECT_EQ(CompositeHash("100500u"), GetFarmFingerprint(MakeUnversionedUint64Value(100500)));


    EXPECT_EQ(CompositeHash("3.25"), GetFarmFingerprint(MakeUnversionedDoubleValue(3.25)));

    EXPECT_EQ(CompositeHash("%true"), GetFarmFingerprint(MakeUnversionedBooleanValue(true)));
    EXPECT_EQ(CompositeHash("%false"), GetFarmFingerprint(MakeUnversionedBooleanValue(false)));
    EXPECT_EQ(CompositeHash("#"), GetFarmFingerprint(MakeUnversionedNullValue()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
