#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ypath/public.h>
#include <yt/yt/core/yson/string.h>

#include <yt/yt/orm/library/attributes/helpers.h>
#include <yt/yt/orm/library/attributes/merge_attributes.h>
#include <yt/yt/orm/library/attributes/unwrapping_consumer.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString ConsumingMergeAttributes(std::vector<TAttributeValue> values)
{
    std::ranges::sort(values, /*comparator*/ {}, /*projection*/ &TAttributeValue::Path);
    ValidateSortedPaths(values, &TAttributeValue::Path, &TAttributeValue::IsEtc);

    TYsonStringWriterHelper writeHelper(NYson::EYsonFormat::Text);
    TMergeAttributesHelper mergeHelper(writeHelper.GetConsumer());
    for (const auto& value : values) {
        mergeHelper.ToNextPath(value.Path, value.IsEtc);
        if (value.IsEtc) {
            TUnwrappingConsumer unwrappingConsumer(writeHelper.GetConsumer());
            unwrappingConsumer.OnRaw(value.Value.AsStringBuf(), value.Value.GetType());
        } else {
            writeHelper.GetConsumer()->OnRaw(value.Value);
        }
    }
    mergeHelper.Finalize();
    return writeHelper.Flush();
}

//////////////////////////////////////////////////////////////////////////////

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

TEST(TValidateSortedPathsTest, SimpleOk)
{
    std::vector<TAttributeValue> attributes = {
        TAttributeValue{
            .Path = "/data",
            .IsEtc = false,
        },
        TAttributeValue{
            .Path = "/key",
            .IsEtc = true,
        },
        TAttributeValue{
            .Path = "/key/inner",
            .IsEtc = false,
        }
    };

    ASSERT_NO_THROW(ValidateSortedPaths(attributes, &TAttributeValue::Path, &TAttributeValue::IsEtc));
}

TEST(TValidateSortedPathsTest, SimpleError)
{
    std::vector<TAttributeValue> attributes = {
        TAttributeValue{
            .Path = "/data",
            .IsEtc = false,
        },
        TAttributeValue{
            .Path = "/data",
            .IsEtc = false,
        }
    };

    ASSERT_ANY_THROW(ValidateSortedPaths(attributes, &TAttributeValue::Path, &TAttributeValue::IsEtc));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSortAndRemoveNestedPathsTest, Root)
{
    std::vector<NYPath::TYPath> paths = {"", "/foo"};

    SortAndRemoveNestedPaths(paths);

    std::vector<NYPath::TYPath> expectedResult = {""};
    ASSERT_EQ(paths, expectedResult);
}

TEST(TSortAndRemoveNestedPathsTest, NonIntersecting)
{
    std::vector<NYPath::TYPath> paths = {"/foo", "/bar"};

    SortAndRemoveNestedPaths(paths);

    std::vector<NYPath::TYPath> expectedResult = {"/bar", "/foo"};
    ASSERT_EQ(paths, expectedResult);
}

TEST(TSortAndRemoveNestedPathsTest, Intersecting)
{
    std::vector<NYPath::TYPath> paths = {"/foo", "/bar", "/foo/bar"};

    SortAndRemoveNestedPaths(paths);

    std::vector<NYPath::TYPath> expectedResult = {"/bar", "/foo"};
    ASSERT_EQ(paths, expectedResult);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TConsumingMergeTest, Simple)
{
    auto result = ConsumingMergeAttributes({
        TAttributeValue{
            .Path = "/data",
            .Value = NYson::TYsonString{R"({})"sv},
            .IsEtc = false,
        }
    });
    ASSERT_EQ(result.ToString(), R"({"data"={};})");
}

TEST(TConsumingMergeTest, Multiple)
{
    auto result = ConsumingMergeAttributes({
        TAttributeValue{
            .Path = "/data",
            .Value = NYson::TYsonString{R"({})"sv},
            .IsEtc = false,
        },
        TAttributeValue{
            .Path = "/key",
            .Value = NYson::TYsonString{R"("value")"sv},
            .IsEtc = false,
        }
    });
    ASSERT_EQ(result.ToString(), R"({"data"={};"key"="value";})");
}

TEST(TConsumingMergeTest, Etc)
{
    auto result = ConsumingMergeAttributes({
        TAttributeValue{
            .Path = "",
            .Value = NYson::TYsonString{R"({"key"="value";})"sv},
            .IsEtc = true,
        },
    });
    ASSERT_EQ(result.ToString(), R"({"key"="value";})");
}

TEST(TConsumingMergeTest, MultiEtc)
{
    auto result = ConsumingMergeAttributes({
        TAttributeValue{
            .Path = "",
            .Value = NYson::TYsonString{R"({"key1"=#;})"sv},
            .IsEtc = true,
        },
        TAttributeValue{
            .Path = "",
            .Value = NYson::TYsonString{R"({"key2"=#;})"sv},
            .IsEtc = true,
        },
    });
    ASSERT_EQ(result.ToString(), R"({"key1"=#;"key2"=#;})");
}

TEST(TConsumingMergeTest, MultipleWithEtc)
{
    auto result = ConsumingMergeAttributes({
        TAttributeValue{
            .Path = "",
            .Value = NYson::TYsonString{R"({"key"="value";})"sv},
            .IsEtc = true,
        },
        TAttributeValue{
            .Path = "/data",
            .Value = NYson::TYsonString{R"({})"sv},
            .IsEtc = true,
        },
        TAttributeValue{
            .Path = "/data",
            .Value = NYson::TYsonString{R"({"key"="value";})"sv},
            .IsEtc = true,
        },
        TAttributeValue{
            .Path = "/data/data",
            .Value = NYson::TYsonString{R"("value")"sv},
            .IsEtc = false,
        },
    });
    ASSERT_EQ(result.ToString(), R"({"key"="value";"data"={"key"="value";"data"="value";};})");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NOrm::NClient::NObjects::NTests
