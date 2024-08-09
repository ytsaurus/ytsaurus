#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAttributePathMatchResult,
    (None)
    (PatternIsPrefix)
    (PathIsPrefix)
    (Full)
);

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((Empty)                    (70001))
    ((MalformedPath)            (70002))
    ((MissingField)             (70004))
    ((MissingKey)               (70005))
    ((InvalidData)              (70006))
    ((OutOfBounds)              (70007))
    ((MismatchingDescriptors)   (70008))
    ((MismatchingPresence)      (70009))
    ((MismatchingSize)          (70010))
    ((MismatchingKeys)          (70011))
    ((Unimplemented)            (70012))
);

////////////////////////////////////////////////////////////////////////////////

template <typename TWrappedMessage>
struct TProtoVisitorTraits;

template <typename TWrappedMessage, typename TSelf>
class TProtoVisitor;

struct TIndexParseResult;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
