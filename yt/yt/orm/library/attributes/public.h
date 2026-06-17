#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/misc/enum.h>

// Forward declarations for google::protobuf and NProtoBuf.
namespace google {
namespace protobuf {

////////////////////////////////////////////////////////////////////////////////

class Arena;
class Message;
class MessageLite;
class Descriptor;
class FieldDescriptor;
class UnknownFieldSet;

////////////////////////////////////////////////////////////////////////////////

namespace util {

////////////////////////////////////////////////////////////////////////////////

class MessageDifferencer;

////////////////////////////////////////////////////////////////////////////////

} // namespace util

////////////////////////////////////////////////////////////////////////////////

} // namespace protobuf
} // namespace google

namespace NProtoBuf {

////////////////////////////////////////////////////////////////////////////////

using ::google::protobuf::Arena;
using ::google::protobuf::Message;
using ::google::protobuf::MessageLite;
using ::google::protobuf::util::MessageDifferencer;
using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::UnknownFieldSet;

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

// Relative indexes (|begin|, |end|, |before:| and |after:|) indicate positions at ends or between
// entries in vectors and repeated fields. Using them to access existing entries (e.g., trying to
// remove such a position) makes a malformed request.
DEFINE_ENUM(ERelativeIndexPolicy,
    (Allow)       // Pass the index to the appropriate handler.
    (Throw)       // Throw an error.
    (Reinterpret) // Convert to absolute index. COMPAT.
);

DEFINE_ENUM(EMergeAttributesMode,
    (Old)
    (New)
    (Compare)
    (CompareCallback)
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
