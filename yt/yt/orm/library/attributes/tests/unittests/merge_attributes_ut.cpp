#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/orm/library/attributes/merge_attributes.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TMergeAttributesTest, ListForwardSimple)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"(["a"; "b"; "c"])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/d/*/e", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"d"=[{"e"="a";};{"e"="b";};{"e"="c";};];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, ListForwardRoot)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"(["a"; "b"; "c"])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/d/*", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"d"=["a";"b";"c";];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, ListForwardNested)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"([["a"; "b";]; ["c"; "d"]; ["e"; "f"];])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/g/*/h/*/i", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"g"=[{"h"=[{"i"="a";};{"i"="b";};];};{"h"=[{"i"="c";};{"i"="d";};];};{"h"=[{"i"="e";};{"i"="f";};];};];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, ListForwardNestedLists)
{
    using namespace std::literals;
    NYson::TYsonString element0YsonStringBuf{R"([["a"; "b";]; ["c"; "d"]; ["e"; "f"];])"sv};

    auto mergedYsonString = MergeAttributes(
        {{.Path = "/g/*/*/h", .Value = element0YsonStringBuf}},
        NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"g"=[[{"h"="a";};{"h"="b";};];[{"h"="c";};{"h"="d";};];[{"h"="e";};{"h"="f";};];];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, EtcWithParent)
{
    using namespace std::literals;
    NYson::TYsonString etc0YsonStringBuf{R"({"a"="c";})"sv};
    NYson::TYsonString etc1YsonStringBuf{R"({"b"="d";})"sv};
    NYson::TYsonString element0YsonStringBuf{R"({"etc"={"f"="g";};})"sv};
    NYson::TYsonString element1YsonStringBuf{R"({"h"=17;})"sv};

    auto mergedYsonString = MergeAttributes({
            {.Path = "/etc", .Value = etc0YsonStringBuf, .IsEtc=true},
            {.Path = "/etc", .Value = etc1YsonStringBuf, .IsEtc=true},
            {.Path = "", .Value = element0YsonStringBuf},
            {.Path = "/etc/i", .Value = element1YsonStringBuf},
        }, NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"etc"={"a"="c";"b"="d";"f"="g";"i"={"h"=17;};};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

TEST(TMergeAttributesTest, Etc)
{
    using namespace std::literals;
    NYson::TYsonString etc0YsonStringBuf{R"({"a"="c";})"sv};
    NYson::TYsonString etc1YsonStringBuf{R"({"b"="d";})"sv};

    auto mergedYsonString = MergeAttributes({
            {.Path = "/etc", .Value = etc0YsonStringBuf, .IsEtc=true},
            {.Path = "/etc", .Value = etc1YsonStringBuf, .IsEtc=true},
        }, NYson::EYsonFormat::Text);
    NYson::TYsonString expectedYsonString{R"({"etc"={"a"="c";"b"="d";};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NObjects::NTests
