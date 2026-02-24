#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt_proto/yt/core/misc/proto/error.pb.h>

#include <algorithm>
#include <iostream>

namespace NYT {
namespace {

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
    for (int i = Depth - 1; i > 0; --i) {
        error = TError(TErrorCode(i), "error") << std::move(error);
    }

    NYT::NProto::TError protoError;
    ToProto(&protoError, error);

    TError deserializedError;
    FromProto(&deserializedError, protoError);

    int countInnerErrorsDepth = GetMaxDepth(deserializedError);
    ASSERT_EQ(countInnerErrorsDepth, ErrorSerializationDepthLimit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT