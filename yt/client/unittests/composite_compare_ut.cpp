#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/composite_compare.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
