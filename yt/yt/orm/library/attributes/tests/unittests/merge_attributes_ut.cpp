#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/orm/library/attributes/merge_attributes.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(MergeAttributes, ListForwardSimple)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"(["a"; "b"; "c"])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/d/*/e", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"d"=[{"e"="a";};{"e"="b";};{"e"="c";};];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(MergeAttributes, ListForwardRoot)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"(["a"; "b"; "c"])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/d/*", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"d"=["a";"b";"c";];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(MergeAttributes, ListForwardNested)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"([["a"; "b";]; ["c"; "d"]; ["e"; "f"];])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/g/*/h/*/i", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"g"=[{"h"=[{"i"="a";};{"i"="b";};];};{"h"=[{"i"="c";};{"i"="d";};];};{"h"=[{"i"="e";};{"i"="f";};];};];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(MergeAttributes, ListForwardNestedLists)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"([["a"; "b";]; ["c"; "d"]; ["e"; "f"];])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/g/*/*/h", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"g"=[[{"h"="a";};{"h"="b";};];[{"h"="c";};{"h"="d";};];[{"h"="e";};{"h"="f";};];];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NObjects::NTests
