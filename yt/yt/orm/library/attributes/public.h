#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/misc/enum.h>

// Forward declarations for google::protobuf and NProtoBuf.
namespace google {
namespace protobuf {

////////////////////////////////////////////////////////////////////////////////

class Message;
class MessageLite;
class Descriptor;
class FieldDescriptor;
class UnknownFieldSet;

////////////////////////////////////////////////////////////////////////////////

} // namespace protobuf
} // namespace google

namespace NProtoBuf {

////////////////////////////////////////////////////////////////////////////////

using Message = ::google::protobuf::Message;
using MessageLite = ::google::protobuf::MessageLite;
using Descriptor = ::google::protobuf::Descriptor;
using FieldDescriptor = ::google::protobuf::FieldDescriptor;
using UnknownFieldSet = ::google::protobuf::UnknownFieldSet;

////////////////////////////////////////////////////////////////////////////////

} // namespace NProtoBuf

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

// How to proceed when the path leads into missing fields.
DEFINE_ENUM(EMissingFieldPolicy,
    (Throw)     // Throw an error.
    (Skip)      // Quietly return.
    (Force)     // Visit the field anyway (visit the default if const/populate the field if mutable).
    (ForceLeaf) // Visit the leaf field anyway, otherwise `Throw`.
);

////////////////////////////////////////////////////////////////////////////////

template <typename TWrappedMessage>
struct TProtoVisitorTraits;

template <typename TWrappedMessage, typename TSelf>
class TProtoVisitor;

struct TIndexParseResult;

////////////////////////////////////////////////////////////////////////////////

class TWireString;
class TWireStringPart;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
