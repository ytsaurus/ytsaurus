#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/misc/identifier.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

#include <util/stream/str.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_IDENTIFIER_TYPEDEF(TTestId);

////////////////////////////////////////////////////////////////////////////////

TEST(TIdentifierTest, DefaultConstruct)
{
    TTestId id;
    EXPECT_EQ(id.Underlying(), "");
    EXPECT_TRUE(id == TTestId{});
}

TEST(TIdentifierTest, ConstructFromStringView)
{
    TTestId id("hello");
    EXPECT_EQ(id.Underlying(), "hello");
}

TEST(TIdentifierTest, CopySharesData)
{
    TTestId a("hello");
    TTestId b = a; // NOLINT(performance-unnecessary-copy-initialization)
    EXPECT_EQ(a.Underlying(), b.Underlying());
    EXPECT_EQ(a.Underlying().data(), b.Underlying().data()); // Same pointer — immutable shared data.
}

TEST(TIdentifierTest, MoveLeavesSenderEmpty)
{
    TTestId a("hello");
    TTestId b = std::move(a);
    EXPECT_EQ(b.Underlying(), "hello");
    EXPECT_EQ(a.Underlying(), ""); // NOLINT(bugprone-use-after-move) Moved-from is empty.
}

TEST(TIdentifierTest, MoveAssignLeavesSenderEmpty)
{
    TTestId a("hello");
    TTestId b;
    b = std::move(a);
    EXPECT_EQ(b.Underlying(), "hello");
    EXPECT_EQ(a.Underlying(), ""); // NOLINT(bugprone-use-after-move) Moved-from must be empty.
}

TEST(TIdentifierTest, SelfMoveAssign)
{
    TTestId a("hello");
    TTestId& ref = a;
    a = std::move(ref); // Must not crash or corrupt.
    EXPECT_EQ(a.Underlying(), "hello");
}

TEST(TIdentifierTest, ComparisonOperators)
{
    TTestId a("aaa"), b("bbb"), a2("aaa");
    EXPECT_TRUE(a == a2);
    EXPECT_FALSE(a == b);
    EXPECT_TRUE(a != b);
    EXPECT_TRUE(a < b);
    EXPECT_FALSE(b < a);
    EXPECT_TRUE(b > a);
    EXPECT_TRUE(a <= a2);
    EXPECT_TRUE(a <= b);
    EXPECT_TRUE(b >= a);
    EXPECT_TRUE(a >= a2);
}

TEST(TIdentifierTest, CompareWithStringView)
{
    TTestId id("hello");
    EXPECT_TRUE(id == std::string_view("hello"));
    EXPECT_FALSE(id == std::string_view("world"));
}

TEST(TIdentifierTest, SaveLoad)
{
    TTestId original("save-load-test");
    TString buf;
    {
        TStringOutput out(buf);
        original.Save(&out);
    }
    TTestId loaded;
    {
        TStringInput in(buf);
        loaded.Load(&in);
    }
    EXPECT_EQ(original, loaded);
}

TEST(TIdentifierTest, ToProtoFromProto)
{
    TTestId original("proto-test");
    TProtobufString serialized;
    ToProto(&serialized, original);
    TTestId result;
    FromProto(&result, serialized);
    EXPECT_EQ(original, result);
}

TEST(TIdentifierTest, SerializeDeserializeYson)
{
    TTestId original("yson-test");
    auto yson = NYson::ConvertToYsonString(original);
    auto result = NYTree::ConvertTo<TTestId>(yson);
    EXPECT_EQ(original, result);
}

TEST(TIdentifierTest, LongString)
{
    // Covers the long-header buffer layout (length > one-byte max).
    const std::string longStr(4096, 'x');
    TTestId id(std::string_view{longStr});
    EXPECT_EQ(id.Underlying(), longStr);
    EXPECT_EQ(id.Underlying().size(), longStr.size());

    TTestId copy = id; // NOLINT(performance-unnecessary-copy-initialization)
    EXPECT_EQ(copy.Underlying().data(), id.Underlying().data());

    TString buf;
    {
        TStringOutput out(buf);
        id.Save(&out);
    }
    TTestId loaded;
    {
        TStringInput in(buf);
        loaded.Load(&in);
    }
    EXPECT_EQ(id, loaded);
}

TEST(TIdentifierHashTest, HashConsistency)
{
    const std::string_view s = "some-id";
    TTestId id(s);
    // The hash is cached at construction as THash of the underlying string, so it stays transparent
    // with THash<std::string_view>. std::hash is intentionally not provided (would diverge).
    EXPECT_EQ(THash<TTestId>{}(id), THash<std::string_view>{}(s));
    EXPECT_EQ(THash<TTestId>{}(TTestId()), THash<std::string_view>{}(std::string_view()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
