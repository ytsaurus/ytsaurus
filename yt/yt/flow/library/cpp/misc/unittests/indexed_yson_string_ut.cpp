#include <yt/yt/flow/library/cpp/misc/indexed_yson_string.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TIndexedYsonStringTest, MatchesGetNodeByYPath)
{
    auto full = ConvertToNode(TYsonString(TStringBuf("{a={b={c=42;d=[1;2;3];};e=\"hello\";};f=[{x=1};{y=2}];g=#;}")));
    // Works on any yson: binary, and pretty text whose tokens are separated by whitespace/newlines.
    for (auto format : {EYsonFormat::Binary, EYsonFormat::Text, EYsonFormat::Pretty}) {
        auto doc = ConvertToYsonString(full, format);
        // 0 forces every map to be parsed; a huge threshold keeps the whole doc as one leaf.
        for (i64 threshold : {0, 5, 25, 1000000}) {
            auto node = TIndexedYsonString::Build(doc, threshold);
            for (const auto& path : {"", "/a", "/a/b", "/a/b/c", "/a/b/d", "/a/b/d/0", "/a/b/d/2", "/a/e", "/f", "/f/1", "/f/1/y", "/g"}) {
                auto expected = GetNodeByYPath(full, path);
                auto actual = ConvertToNode(node->GetByPath(path));
                EXPECT_TRUE(AreNodesEqual(expected, actual))
                    << "path: " << path << ", threshold: " << threshold << ", format: " << static_cast<int>(format);
            }
        }
    }
}

TEST(TIndexedYsonStringTest, InvalidPathThrows)
{
    auto doc = ConvertToYsonString(ConvertToNode(TYsonString(TStringBuf("{a={b=1;};c=[10;20;];}"))));
    for (i64 threshold : {0, 1000000}) {
        auto node = TIndexedYsonString::Build(doc, threshold);
        EXPECT_THROW(node->GetByPath("/a/z"), std::exception);
        EXPECT_THROW(node->GetByPath("/x"), std::exception);
        EXPECT_THROW(node->GetByPath("/a/b/c"), std::exception);
        EXPECT_THROW(node->GetByPath("/c/5"), std::exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
