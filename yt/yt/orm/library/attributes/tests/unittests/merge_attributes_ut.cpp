#include <yt/yt/orm/library/attributes/merge_attributes.h>
#include <yt/yt/orm/library/attributes/unwrapping_consumer.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/yson/yson_builder.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NOrm::NAttributes::NTests {
namespace {

using namespace std::literals;

////////////////////////////////////////////////////////////////////////////////

struct TForcePath
{
    NYPath::TYPath Path;
    bool Force;

    bool operator==(const TForcePath& other) const = default;
};

void FormatValue(::NYT::TStringBuilderBase* builder, const TForcePath& path, TStringBuf /*spec*/)
{
    Format(builder, "{Path: %v, Force: %v}", path.Path, path.Force);
}

void PrintTo(const TForcePath& path, std::ostream* os)
{
    *os << ToString(path);
}

////////////////////////////////////////////////////////////////////////////////

NYson::TYsonString ConsumingMergeAttributes(std::vector<TAttributeValue> values)
{
    std::ranges::sort(values, /*comparator*/ {}, /*projection*/ &TAttributeValue::Path);
    ValidateSortedPaths(values, &TAttributeValue::Path, &TAttributeValue::IsEtc);

    NYson::TYsonStringBuilder builder(NYson::EYsonFormat::Text);
    TMergeAttributesHelper mergeHelper(builder.GetConsumer());
    for (const auto& value : values) {
        mergeHelper.ToNextPath(value.Path, value.IsEtc);
        if (value.IsEtc) {
            TUnwrappingConsumer unwrappingConsumer(builder.GetConsumer());
            unwrappingConsumer.OnRaw(value.Value.AsStringBuf(), value.Value.GetType());
        } else {
            builder->OnRaw(value.Value);
        }
    }
    mergeHelper.Finalize();
    return builder.Flush();
}

NYson::TYsonString NewMergeAttributes(std::vector<TAttributeValue> attributeValues) {
    return MergeAttributes(
        std::move(attributeValues),
        NYson::EYsonFormat::Text,
        EDuplicatePolicy::PrioritizeColumn,
        EMergeAttributesMode::Compare);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMergeAttributesTest, ListForwardSimple)
{
    NYson::TYsonString element0YsonStringBuf{R"(["a"; "b"; "c"])"sv};

    auto mergedYsonString = NewMergeAttributes(
        {{.Path = "/d/*/e", .Value = element0YsonStringBuf}});
    NYson::TYsonString expectedYsonString{R"({"d"=[{"e"="a";};{"e"="b";};{"e"="c";};];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, ListForwardRoot)
{
    NYson::TYsonString element0YsonStringBuf{R"(["a"; "b"; "c"])"sv};

    auto mergedYsonString = NewMergeAttributes(
        {{.Path = "/d/*", .Value = element0YsonStringBuf}});
    NYson::TYsonString expectedYsonString{R"({"d"=["a";"b";"c";];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, ListForwardNested)
{
    NYson::TYsonString element0YsonStringBuf{R"([["a"; "b";]; ["c"; "d"]; ["e"; "f"];])"sv};

    auto mergedYsonString = NewMergeAttributes(
        {{.Path = "/g/*/h/*/i", .Value = element0YsonStringBuf}});
    NYson::TYsonString expectedYsonString{R"({"g"=[{"h"=[{"i"="a";};{"i"="b";};];};{"h"=[{"i"="c";};{"i"="d";};];};{"h"=[{"i"="e";};{"i"="f";};];};];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, ListForwardNestedLists)
{
    NYson::TYsonString element0YsonStringBuf{R"([["a"; "b";]; ["c"; "d"]; ["e"; "f"];])"sv};

    auto mergedYsonString = NewMergeAttributes(
        {{.Path = "/g/*/*/h", .Value = element0YsonStringBuf}});
    NYson::TYsonString expectedYsonString{R"({"g"=[[{"h"="a";};{"h"="b";};];[{"h"="c";};{"h"="d";};];[{"h"="e";};{"h"="f";};];];})"sv};
    EXPECT_EQ(mergedYsonString, expectedYsonString);
}

TEST(TMergeAttributesTest, EtcWithParent)
{
    NYson::TYsonString etc0YsonStringBuf{R"({"a"="c";})"sv};
    NYson::TYsonString etc1YsonStringBuf{R"({"b"="d";})"sv};
    NYson::TYsonString element0YsonStringBuf{R"({"etc"={"f"="g";};})"sv};
    NYson::TYsonString element1YsonStringBuf{R"({"h"=17;})"sv};

    auto mergedYsonString = NewMergeAttributes({
            {.Path = "/etc", .Value = etc0YsonStringBuf, .IsEtc = true},
            {.Path = "/etc", .Value = etc1YsonStringBuf, .IsEtc = true},
            {.Path = "", .Value = element0YsonStringBuf},
            {.Path = "/etc/i", .Value = element1YsonStringBuf},
        });
    NYson::TYsonString expectedYsonString{R"({"etc"={"f"="g";"a"="c";"b"="d";"i"={"h"=17;};};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

TEST(TMergeAttributesTest, HasPrefixes)
{
    NYson::TYsonString aYsonStringBuf{R"({"b"={"c"="10"}})"sv};
    NYson::TYsonString abdYsonStringBuf{R"(20)"sv};
    NYson::TYsonString acYsonStringBuf{R"({"e"="30"})"sv};


    auto mergedYsonString = NewMergeAttributes({
            {.Path = "/a", .Value = aYsonStringBuf},
            {.Path = "/a/b/d", .Value = abdYsonStringBuf},
            {.Path = "/a/c", .Value = acYsonStringBuf},
        });
    NYson::TYsonString expectedYsonString{R"({"a"={"b"={"c"="10";"d"=20;};"c"={"e"="30";};};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

TEST(TMergeAttributesTest, DeepPathWithPrefixes)
{
    NYson::TYsonString abcdeYsonStringBuf{R"({"a"=1})"sv};
    NYson::TYsonString bYsonStringBuf{R"({"c"={"d"=20}})"sv};
    NYson::TYsonString bceYsonStringBuf{R"({"x"={"y"={"z"=2;}}})"sv};
    NYson::TYsonString bcfYsonStringBuf{R"(4)"sv};
    NYson::TYsonString cYsonStringBuf{R"({"d"={"e"={"x"=3;};};f=3})"sv};
    NYson::TYsonString cdeyYsonStringBuf{R"("value")"sv};

    auto mergedYsonString = NewMergeAttributes({
            {.Path = "/a/b/c/d/e", .Value = abcdeYsonStringBuf},
            {.Path = "/b", .Value = bYsonStringBuf},
            {.Path = "/b/c/e", .Value = bceYsonStringBuf},
            {.Path = "/b/c/f", .Value = bcfYsonStringBuf},
            {.Path = "/c", .Value = cYsonStringBuf},
            {.Path = "/c/d/e/y", .Value = cdeyYsonStringBuf},
        });
    NYson::TYsonString expectedYsonString{
        R"({"a"={"b"={"c"={"d"={"e"={"a"=1;};};};};};"b"={"c"={"d"=20;"e"={"x"={"y"={"z"=2;};};};"f"=4;};};"c"={"d"={"e"={"x"=3;"y"="value";};};"f"=3;};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

TEST(TMergeAttributesTest, Etc)
{
    NYson::TYsonString etc0YsonStringBuf{R"({"a"="c";})"sv};
    NYson::TYsonString etc1YsonStringBuf{R"({"b"="d";})"sv};

    auto mergedYsonString = NewMergeAttributes({
            {.Path = "/etc", .Value = etc0YsonStringBuf, .IsEtc = true},
            {.Path = "/etc", .Value = etc1YsonStringBuf, .IsEtc = true},
        });
    NYson::TYsonString expectedYsonString{R"({"etc"={"a"="c";"b"="d";};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

TEST(TMergeAttributesTest, DuplicateValues)
{
    NYson::TYsonString elementYsonStringBuf{"b"sv};
    NYson::TYsonString etcYsonStringBuf{R"({"a"="c";})"sv};
    std::vector<TAttributeValue> attributeValues = {
        {.Path = "/x/a", .Value = elementYsonStringBuf},
        {.Path = "/x", .Value = etcYsonStringBuf, .IsEtc = true},
    };

    {
        auto mergedYsonString = NewMergeAttributes(attributeValues);
        NYson::TYsonString expectedYsonString{R"({"x"={"a"="b";};})"sv};
        EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
    }
    {
        auto mergedYsonString = MergeAttributes(
            attributeValues,
            NYson::EYsonFormat::Text,
            EDuplicatePolicy::PrioritizeEtc,
            EMergeAttributesMode::Compare);
        NYson::TYsonString expectedYsonString{R"({"x"={"a"="c";};})"sv};
        EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
    }
}

TEST(TMergeAttributesTest, DeepPathWithNull)
{
    NYson::TYsonString nullYsonStringBuf{R"(#)"sv};

    auto mergedYsonString = NewMergeAttributes({
            {.Path = "/a/b", .Value = nullYsonStringBuf},
            {.Path = "/a/b/c/d/e", .Value = nullYsonStringBuf},
            {.Path = "/b", .Value = nullYsonStringBuf},
            {.Path = "/b/c/e", .Value = nullYsonStringBuf},
            {.Path = "/b/c/f", .Value = nullYsonStringBuf},
            {.Path = "/c", .Value = nullYsonStringBuf},
            {.Path = "/c/d/e/y", .Value = nullYsonStringBuf},
        });
    NYson::TYsonString expectedYsonString{
        R"({"a"={"b"={"c"={"d"={"e"=#;};};};};"b"={"c"={"e"=#;"f"=#;};};"c"={"d"={"e"={"y"=#;};};};})"sv};
    EXPECT_EQ(mergedYsonString.AsStringBuf(), expectedYsonString.AsStringBuf());
}

TEST(TMergeAttributesTest, MapInsideList)
{
    NYson::TYsonString root{R"({"a"=[{"v"={}};{}]})"sv};
    NYson::TYsonString vBar{R"(#)"sv};
    auto mergedYsonString = NewMergeAttributes({
        {.Path = "", .Value = root},
        {.Path = "/v/bar", .Value = vBar},
    });
    EXPECT_EQ(TStringBuf{R"({"a"=[{"v"={};};{};];"v"={"bar"=#;};})"sv}, mergedYsonString.AsStringBuf());
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

TEST(TSortAndRemoveNestedPathsTest, Fixed)
{
    std::vector<TForcePath> paths = {
        {.Path = "/foo", .Force = false},
        {.Path = "/foo/bar", .Force = true},
        {.Path = "/bar", .Force = false},
        {.Path = "/foo/bar/boo", .Force = false}
    };
    SortAndRemoveNestedPaths(paths, &TForcePath::Path, &TForcePath::Force);

    std::vector<TForcePath> expectedResult = {
        {.Path = "/bar", .Force = false},
        {.Path = "/foo", .Force = false},
        {.Path = "/foo/bar", .Force = true}
    };
    ASSERT_EQ(paths, expectedResult);
}

TEST(TSortAndRemoveNestedPathsTest, FixedFlat)
{
    std::vector<TForcePath> paths = {
        {.Path = "", .Force = false},
        {.Path = "/foo", .Force = true},
        {.Path = "/bar", .Force = true},
        {.Path = "/foobar", .Force = false}
    };
    SortAndRemoveNestedPaths(paths, &TForcePath::Path, &TForcePath::Force);

    std::vector<TForcePath> expectedResult = {
        {.Path = "", .Force = false},
        {.Path = "/bar", .Force = true},
        {.Path = "/foo", .Force = true},
    };
    ASSERT_EQ(paths, expectedResult);
}

TEST(TSortAndRemoveNestedPathsTest, FixedChild)
{
    std::vector<TForcePath> paths = {
        {.Path = "", .Force = false},
        {.Path = "/bar", .Force = true},
        {.Path = "/foo", .Force = true},
        {.Path = "/foo/bar", .Force = false},
        {.Path = "/foo/bar/k", .Force = true}
    };
    SortAndRemoveNestedPaths(paths, &TForcePath::Path, &TForcePath::Force);

    std::vector<TForcePath> expectedResult = {
        {.Path = "", .Force = false},
        {.Path = "/bar", .Force = true},
        {.Path = "/foo", .Force = true},
        {.Path = "/foo/bar/k", .Force = true}
    };
    ASSERT_EQ(paths, expectedResult);
}

TEST(TSortAndRemoveNestedPathsTest, RepeatedOpaque)
{
    std::vector<TForcePath> paths = {
        {.Path = "", .Force = false},
        {.Path = "/bar", .Force = true},
        {.Path = "/bar", .Force = true},
    };
    SortAndRemoveNestedPaths(paths, &TForcePath::Path, &TForcePath::Force);

    std::vector<TForcePath> expectedResult = {
        {.Path = "", .Force = false},
        {.Path = "/bar", .Force = true}
    };
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
} // namespace NYT::NOrm::NAttributes::NTests
