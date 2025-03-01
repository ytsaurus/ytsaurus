#pragma once

#include "public.h"

#include "proto_visitor_traits.h"
#include "path_visitor.h"

#include <library/cpp/yt/misc/property.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

/// Construction kit for pain-free (hopefully) protobuf traversals.
//
// 1. Make your own visitor by subclassing a suitable specialization of TProtoVisitor. Make it final.
//    The template parameter is the fully qualified (with const, ref, or pointer) message wrapper
//    (pointer or container of pointers) supplied to all methods. See traits for actual
//    specializations. Or write your own.
//
// 2. Read the TPathVisitor comments.

/// Design notes.
//
// The base implementation provides for:
// - Directed traversal of a path in a protobuf tree.
// - (Optional) depth-first traversal of asterisks and subtrees after the path.
// - Const and mutable messages.
// - Parallel traversal of containers of messages. Just a template parameter away.
// - Repeated and map fields.
// - Absolute and relative positions in repeated fields.
// - Arbitrary map key types.
// - Checking for presence of singular fields.
//
// The base implementation pays little attention to:
// - Scalars (except map keys). Handle these in appropriate overrides.
// - Oneofs. These are traversed like regular fields.
// - Unknown fields. Handle these in an override of VisitMessage or VisitUnrecognizedField.
// - Extensions. Ditto.
// - Continuation of the path into serialized YSON or proto fields. Yep, you handle them.
//
// When visiting containers of messages, the visitor (well, the traits) recombines containers when
// descending through message fields. The parallel fields must match exactly (same field presence,
// repeated size or map keys). Other behaviors can be implemented in, well, method overrides.
//
// Traits do not throw. Instead, they make liberal use of TErrorOr with detailed error codes. This
// makes sure the implementation can make decisions about various error conditions.
//
// The Visit method comes from TPathVisitor and it is confusing that either TWrappedMessage or
// TVisitParam can be a vector or a map.
//
// Here's the difference:
// - A bunch of messages in TWrappedMessage are distinct message trees that are traversed in
//   parallel. You receive these in the overrides and handle them appropriately. For example,
//   parallel traversal of a pair of messages is a natural way of implementing comparison.
// - A map or vector as the top-level container is the top-level node in one message tree. Its index
//   or key is the first entry in the path. For example, an ORM attribute may be modeled as a
//   repeated field of messages, stored in the database as a YSON list and represented in memory as
//   a vector of messages. The visitor will interpret an asterisk as a request to visit all entries
//   in the vector, ditto for an integer path entry.

template <typename TWrappedMessage, typename TSelf>
class TProtoVisitor
    : public TPathVisitorMixin
    , public TPathVisitor<TSelf>
{
    friend class TPathVisitor<TSelf>;
    using TPathVisitor<TSelf>::Self;

public:
    // Call VisitAttributeDictionary for TAttributeDictionary. Otherwise call VisitRegularMessage.
    DEFINE_BYVAL_RW_PROPERTY(bool, ProcessAttributeDictionary, false);

protected:
    using TTraits = TProtoVisitorTraits<TWrappedMessage>;

    using TMessageParam = typename TTraits::TMessageParam;
    using TMessageReturn = typename TTraits::TMessageReturn;

    // TPathVisitor entry point.
    template <typename TVisitParam>
    void VisitGeneric(TVisitParam&& target, EVisitReason reason);

    /// Message section.
    // Generic message dispatcher. Called for the initial message of the visit and every recursion.
    // Distinguishes special message types (currently, TAttributeDictionary).
    void VisitMessage(TMessageParam message, EVisitReason reason);

    // Called for non-special message types.
    void VisitRegularMessage(
        TMessageParam message,
        const NProtoBuf::Descriptor* descriptor,
        EVisitReason reason);

    // Called for asterisks and visits after the path.
    void VisitWholeMessage(TMessageParam message, EVisitReason reason);
    // The field name with this name was not found in the message. Not to be confused with unknown
    // fields (although the field may be found in the unknown field set).
    // Current message descriptor and unknown field name provided for convenience.
    void VisitUnrecognizedField(
        TMessageParam message,
        const NProtoBuf::Descriptor* descriptor,
        TString name,
        EVisitReason reason);
    // Called when there is a problem with looking up the message descriptor (e.g., mismatching
    // descriptors in a wrap or an empty wrap). Throws the error by default.
    void OnDescriptorError(
        TMessageParam message,
        EVisitReason reason,
        TError error);

    /// AttributeDictionary section.
    // Called when ProcessAttributeDictionary is set and the message has the attribute_dictionary
    // option. The |fieldDescriptor| points to the repeated |attributes| field. Whole/entry/error
    // machinery is not provided; overrides should handle all appropriate situations.
    void VisitAttributeDictionary(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    /// Generic field dispatcher. Calls map/repeated/singular variants.
    void VisitField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    /// Singular field section.
    // Called to visit a plain old singular field. Checks presence and calls
    // Visit[Present|Missing]SingularField or OnPresenceError.
    void VisitSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a present singular field. Also called by default from VisitMapFieldEntry.
    // Default implementation calls VisitMessage or throws.
    void VisitPresentSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a present singular field of a scalar type at the end of the path.
    void VisitScalarSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a missing singular field. Throws unless convinced otherwise by flags and
    // reason.
    void VisitMissingSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called when there is a problem with evaluating field presense (e.g., mismatching presence in
    // a wrap). Throws the error by default.
    void OnPresenceError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error);

    /// Repeated field section.
    // Called for, yes, repeated fields.
    void VisitRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called for asterisks and visits after the path.
    void VisitWholeRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a specific entry in the repeated field. The index is within bounds.
    // Default implementation calls VisitMessage or throws.
    void VisitRepeatedFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);
    // Called to visit a repeated field entry of a scalar type at the end of the path.
    void VisitScalarRepeatedFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);
    // The path contained a relative index. The expected behavior is to insert a new entry *before*
    // the indexed one (so the new entry has the indicated index). The index is within bounds or
    // equals the repeated field size.
    void VisitRepeatedFieldEntryRelative(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);
    // Called when there is a problem with evaluating field size (e.g., mismatching sizes in a
    // wrap). Throws the error by default.
    void OnSizeError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error);
    // Called when there is a problem with evaluating field index (e.g., out of bounds).
    // Throws the error by default unless missing values are allowed.
    void OnIndexError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error);

    /// Map section.
    // Called for, well, map fields.
    void VisitMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called for asterisks and visits after the path.
    void VisitWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // The entry was located. The specific parameters are:
    // - message is the one containing the map
    // - fieldDescriptor describes the map (see its message_type()->map_key() and map_value())
    // - entryMessage is the entry in the map (synthetic message type with key and value)
    // - key is the string representation of the key for convenience.
    //
    // The index in the underlying repeated field cannot be supplied because it does not have to
    // be consistent in containers. Use LocateMapEntry with the entryMessage to manipulate the map
    // by index.
    //
    // Default implementation calls VisitSingularField with the entry message and value field.
    void VisitMapFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        TMessageParam entryMessage,
        TString key,
        EVisitReason reason);
    // There was an error looking up the entry (key not found or mismatching in the wrap). Throws
    // the error by default. The specific parameters are:
    // - message is the one containing the map
    // - fieldDescriptor describes the map (see its message_type()->map_key() and map_value())
    // - keyMessage is the synthetic entry with the key field set used to locate the message;
    //   consider using it if you are creating new entries
    // - key is the string representation of the key for convenience.
    // If the error was seen in VisitWholeMapField, key parameters are not provided.
    void OnKeyError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        std::unique_ptr<NProtoBuf::Message> keyMessage,
        TString key,
        EVisitReason reason,
        TError error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

#define PROTO_VISITOR_INL_H_
#include "proto_visitor-inl.h"
#undef PROTO_VISITOR_INL_H_
