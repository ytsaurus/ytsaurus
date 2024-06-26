#pragma once

#include "public.h"

#include <yt/yt/core/misc/error.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

// Unimplemented generic template provided for documentation.

// Specializations are provided for TWrappedMessage in:
// - Message*
// - const Message*
// - std::pair<[const] Message*, [const] Message*>&
// - std::vector<[const] Message*>&
// - TCompactVector<[const] Message*, N>&
// Null message pointers are allowed and are treated meaningfully by all methods.

template <typename TWrappedMessage>
struct TProtoVisitorTraits
{
    // Type supplied to all methods.
    using TMessageParam = TWrappedMessage;
    // Type returned from appropriate methods.
    using TMessageReturn = std::remove_reference_t<TWrappedMessage>;

    // Checks that all messages have the same descriptor and returns the descriptor.
    static TErrorOr<const NProtoBuf::Descriptor*> GetDescriptor(TMessageParam message);

    // Checks that the indicated singular field in all messages has consistent presence state and
    // returns the presence state.
    static TErrorOr<bool> IsSingularFieldPresent(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor);

    // For each message, returns the message from the indicated present singular field.
    static TMessageReturn GetMessageFromSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor);

    // Checks that the indicated repeated field in all messages has consistent size and returns the
    // size.
    static TErrorOr<int> GetRepeatedFieldSize(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor);

    // For each message, returns the message from the indicated index in the indicated repeated
    // field.
    static TMessageReturn GetMessageFromRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index);

    // For each message, locates the map entry that has the same key field as the supplied
    // prototype. Returns the entry(ies) if the presence of the entry is consistent in all maps.
    static TErrorOr<TMessageReturn> GetMessageFromMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        const NProtoBuf::Message* keyMessage);

    // For each message, loads all entries into a vector and sorts them by key. If all maps are
    // consistent (have the same keys), returns the map entries organized by the string
    // representation of the key.
    using TMapReturn = THashMap<TString, TMessageReturn>;
    static TErrorOr<TMapReturn> GetMessagesFromWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor);
};

////////////////////////////////////////////////////////////////////////////////

// Classification of containers passed to TProtoVisitor::Visit. Make sure to drop qualifications on
// the template parameters with std::remove_cvref_t to avoid mismatches. If the parameter is neither
// scalar, not vector, nor map, Visit throws.

// Specializations are provided for TVisitParam in:
// - TWrappedMessage[*] (scalar)
// - std::vector/TCompactVector<TWrappedMessage[*]> (vector)
// - std::[unordered_]map/THashMap<$TKey, TWrappedMessage[*]> (map)

template <typename TWrappedMessage, typename TVisitParam>
struct TProtoVisitorContainerTraits
{
    static constexpr bool IsScalar = false;
    static constexpr bool IsVector = false;
    static constexpr bool IsMap = false;

    // The actual message is given by value or reference.
    static constexpr bool TakeAddress = false;

    using TMapKey = void;
};

} // namespace NYT::NOrm::NAttributes

#define PROTO_VISITOR_TRAITS_INL_H_
#include "proto_visitor_traits-inl.h"
#undef PROTO_VISITOR_TRAITS_INL_H_
