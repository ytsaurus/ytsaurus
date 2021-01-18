#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/error.h>

#include <yt/core/yson/string.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TErrorTest, SerializationDepthLimit)
{
    constexpr int Depth = 1000;
    auto error = TError(TErrorCode(Depth), "error");
    for (int i = Depth - 1; i >= 0; --i) {
        error = TError(TErrorCode(i), "error") << std::move(error);
    }

    // Use intermediate conversion to test YSON parser depth limit simultaneously.
    auto errorYson = ConvertToYsonString(error);
    auto errorNode = ConvertTo<IMapNodePtr>(errorYson);

    for (int i = 0; i < ErrorSerializationDepthLimit - 1; ++i) {
        ASSERT_EQ(errorNode->GetChildOrThrow("code")->GetValue<i64>(), i);
        ASSERT_EQ(errorNode->GetChildOrThrow("message")->GetValue<TString>(), "error");
        ASSERT_FALSE(errorNode->GetChildOrThrow("attributes")->AsMap()->FindChild("original_error_depth"));
        auto innerErrors = errorNode->GetChildOrThrow("inner_errors")->AsList()->GetChildren();
        ASSERT_EQ(innerErrors.size(), 1);
        errorNode = innerErrors[0]->AsMap();
    }
    auto innerErrors = errorNode->GetChildOrThrow("inner_errors")->AsList();
    const auto& children = innerErrors->GetChildren();
    ASSERT_EQ(children.size(), Depth - ErrorSerializationDepthLimit + 1);
    for (int i = 0; i < children.size(); ++i) {
        auto child = children[i]->AsMap();
        ASSERT_EQ(child->GetChildOrThrow("code")->GetValue<i64>(), i + ErrorSerializationDepthLimit);
        ASSERT_EQ(child->GetChildOrThrow("message")->GetValue<TString>(), "error");
        auto originalErrorDepth = child->GetChildOrThrow("attributes")->AsMap()->FindChild("original_error_depth");
        if (i > 0) {
            ASSERT_TRUE(originalErrorDepth);
            ASSERT_EQ(originalErrorDepth->GetValue<i64>(), i + ErrorSerializationDepthLimit);
        } else {
            ASSERT_FALSE(originalErrorDepth);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

}
} // namespace NYT
