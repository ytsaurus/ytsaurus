#include <yt/yt/orm/library/attributes/asterisk_projection.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/error/error.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

using NYson::TYsonString;
using NYson::EYsonFormat;

void ExpectYsonEq(TStringBuf expected, const TYsonString& actual)
{
    EXPECT_TRUE(NYTree::AreNodesEqual(
        NYTree::ConvertToNode(TYsonString(expected)),
        NYTree::ConvertToNode(actual)))
        << "expected: " << expected << ", actual: " << actual.ToString();
}

TEST(TProjectListAsteriskTest, LeafProjection)
{
    ExpectYsonEq(
        "[1; 42]",
        ProjectListAsterisk(TYsonString(TStringBuf("[{foo=1; bar=2}; {foo=42; bar=3}]")), "/foo"));
}

TEST(TProjectListAsteriskTest, SubmessageProjection)
{
    ExpectYsonEq(
        "[{name=a}; {name=b}]",
        ProjectListAsterisk(TYsonString(TStringBuf("[{sub={name=a}}; {sub={name=b}}]")), "/sub"));
}

TEST(TProjectListAsteriskTest, EmptyPath)
{
    TStringBuf val = "[{foo=1}; {foo=2}]";
    ExpectYsonEq(
        val,
        ProjectListAsterisk(TYsonString(val), ""));
}

TEST(TProjectListAsteriskTest, EmptyList)
{
    ExpectYsonEq("[]", ProjectListAsterisk(TYsonString(TStringBuf("[]")), "/foo"));
}

TEST(TProjectListAsteriskTest, NullInput)
{
    ExpectYsonEq("#", ProjectListAsterisk(TYsonString(TStringBuf("#")), "/foo"));
}

TEST(TProjectListAsteriskTest, MissingFieldBecomesEntity)
{
    ExpectYsonEq(
        "[1; #]",
        ProjectListAsterisk(TYsonString(TStringBuf("[{foo=1}; {bar=2}]")), "/foo"));
}

TEST(TProjectListAsteriskTest, NonMapElementBecomesEntity)
{
    ExpectYsonEq(
        "[#; 5]",
        ProjectListAsterisk(TYsonString(TStringBuf("[7; {foo=5}]")), "/foo"));
}

TEST(TProjectListAsteriskTest, NonListInputThrows)
{
    EXPECT_THROW(ProjectListAsterisk(TYsonString(TStringBuf("123")), "/foo"), NYT::TErrorException);
}

TEST(TProjectListAsteriskTest, MapInputThrows)
{
    EXPECT_THROW(ProjectListAsterisk(TYsonString(TStringBuf("{a=1}")), "/foo"), NYT::TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NAttributes::NTests
