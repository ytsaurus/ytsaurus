#pragma once

#ifndef CHECK_YSON_TOKEN_INL_H_
#error "Direct inclusion of this file is not allowed, include check_yson.h"
// For the sake of sane code completion.
#include "check_yson_token-inl.h"
#endif

#include <yt/core/yson/pull_parser.h>

namespace NYT::NComplexTypes {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void EnsureYsonToken(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    NYson::EYsonItemType actual,
    NYson::EYsonItemType expected)
{
    if (expected != actual) {
        ThrowUnexpectedYsonTokenException(descriptor, actual, {expected});
    }
}

Y_FORCE_INLINE void EnsureYsonToken(
    const NTableClient::TComplexTypeFieldDescriptor& descriptor,
    const NYson::TYsonPullParserCursor& cursor,
    NYson::EYsonItemType expected)
{
    return EnsureYsonToken(descriptor, cursor->GetType(), expected);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NComplexTypes
