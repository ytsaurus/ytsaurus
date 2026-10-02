#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt_proto/yt/core/misc/proto/error.pb.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/attributes.h>
#include <yt/yt/core/ytree/convert.h>

#include <algorithm>
#include <iostream>

namespace NYT {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

int GetMaxDepth(const TError& error, int currentDepth = 0) {
    int maxDepth = currentDepth;
    
    for (const auto& innerError : error.InnerErrors()) {
        maxDepth = std::max(maxDepth, GetMaxDepth(innerError, currentDepth + 1));
    }
    
    return maxDepth;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TErrorProtoTest, SerializationDepthLimit)
{
    constexpr int Depth = 100;
    auto error = TError(TErrorCode(Depth), "error");

    // not using 0 depth because inner error are not serialized to proto if error code is OK
    for (int i = Depth - 1; i > 0; --i) {
        error = TError(TErrorCode(i), "error") << std::move(error);
    }

    NYT::NProto::TError protoError;
    ToProto(&protoError, error);

    TError deserializedError;
    FromProto(&deserializedError, protoError);

    int countInnerErrorsDepth = GetMaxDepth(deserializedError);
    ASSERT_EQ(countInnerErrorsDepth, ErrorSerializationDepthLimit);

    // this is already tested in error_ut.cpp
    auto errorYson = ConvertToYsonString(deserializedError);
    auto errorNode = ConvertTo<IMapNodePtr>(errorYson);

    for (int i = 1; i < ErrorSerializationDepthLimit; ++i) {
        ASSERT_EQ(errorNode->GetChildValueOrThrow<i64>("code"), i);
        ASSERT_EQ(errorNode->GetChildValueOrThrow<TString>("message"), "error");
        ASSERT_FALSE(errorNode->GetChildOrThrow("attributes")->AsMap()->FindChild("original_error_depth"));
        auto innerErrors = errorNode->GetChildOrThrow("inner_errors")->AsList()->GetChildren();
        ASSERT_EQ(innerErrors.size(), 1u);
        errorNode = innerErrors[0]->AsMap();
    }
    auto innerErrors = errorNode->GetChildOrThrow("inner_errors")->AsList();
    const auto& children = innerErrors->GetChildren();
    ASSERT_EQ(std::ssize(children), Depth - ErrorSerializationDepthLimit);
    for (int i = 0; i < std::ssize(children); ++i) {
        auto child = children[i]->AsMap();
        ASSERT_EQ(child->GetChildValueOrThrow<i64>("code"), i + ErrorSerializationDepthLimit + 1);
        ASSERT_EQ(child->GetChildValueOrThrow<TString>("message"), "error");
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

} // namespace
} // namespace NYT
