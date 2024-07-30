#pragma once

#include "public.h"

#include "proto_visitor_traits.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/stack.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <library/cpp/yt/misc/property.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

// A cookie explaining how the decision to call a ProtoVisitor method was made.
DEFINE_ENUM(EVisitReason,
    (TopLevel)  // Visiting the message supplied by the caller.
    (Path)      // Visiting an object indicated by the path.
    (Asterisk)  // Visiting all entries indicated by an asterisk.
    (AfterPath) // Visiting the entire subtree after exhausting the path.
    (Manual)    // Visit out of the ordinary pattern initiated by the implementation.
);

class TProtoVisitorBase
{
public:
    /// Policy flags.
    // Allows a "fragment" path missing the leading slash. COMPAT.
    DEFINE_BYVAL_RW_PROPERTY(bool, LeadingSlashOptional, false);
    // Having reached the end of the tokenizer path, visit everything in the field/map/repeated.
    // Does not throw when visiting absent fields.
    DEFINE_BYVAL_RW_PROPERTY(bool, VisitEverythingAfterPath, false);
    // Do not throw if the path leads into a missing field/key/index.
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowMissing, false);
    // Visit all fields/entries when the path has a "*".
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowAsterisk, false);
    // Call VisitAttributeDiectionary for TAttributeDictionary. Otherwise call VisitRegularMessage.
    DEFINE_BYVAL_RW_PROPERTY(bool, ProcessAttributeDictionary, false);

protected:
    // Maintains the path supplied by the caller.
    NYPath::TTokenizer Tokenizer_;
    // Maintains the current path of the visitor. Will differ from the path in the Tokenizer:
    // - When traversing asterisks and items after the path (actual keys are substituted),
    // - When traversing negative and relative indices (actual indices are substituted),
    // - When manipulated by the concrete visitor (it is recommended to maintain the path when
    //   manually controlling the visitor).
    NYPath::TYPathStack CurrentPath_;

    /// Control flags.
    // Breaks out of asterisk and afterpath loops for the rest of the visit.
    bool StopIteration_ = false;

    /// Tokenizer management.
    // Advances tokenizer over a slash unless it's optional here.
    void SkipSlash();

    using TToken = std::variant<int, TStringBuf>;

    // Pushes the token onto the current path.
    void Push(TToken token);

    // Advances the tokenizer and pushes the token onto the current path.
    virtual void AdvanceOver(TToken token);

    // Throws if we don't support asterisks. Advances the tokenizer.
    virtual void AdvanceOverAsterisk();

    class TCheckpoint
    {
    public:
        explicit TCheckpoint(NYPath::TTokenizer& tokenizer);
        ~TCheckpoint();

        void Defer(std::function<void()> defer);

    private:
        NYPath::TTokenizer::TCheckpoint TokenizerCheckpoint_;
        std::function<void()> Defer_;
    };

    // Called when visiting a path that can branch (asterisks and "after path" traversals).
    // Pushes the token onto the current path.
    // Checkpoint destructor restores the tokenizer and current path.
    virtual TCheckpoint CheckpointBranchedTraversal(TToken token);

    // Throws if the token type is wrong.
    void Expect(NYPath::ETokenType type) const;
    // Returns true if the tokenizer has completed the path.
    bool PathComplete() const;
    // Prepares working set for a new traversal.
    virtual void Reset(NYPath::TYPathBuf path);

    /// Index management.
    // Computes the repeated field index from the current token.
    TErrorOr<TIndexParseResult> ParseCurrentListIndex(int size) const;
    // Generates a map entry message with the key converted to appropriate type and filled in.
    std::unique_ptr<NProtoBuf::Message> MakeMapKeyMessage(
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        const TString& key) const;

    /// Error management.
    // Same as error.h but uses our throw.
    template <typename TValue>
    TValue ValueOrThrow(TErrorOr<TValue> errorOrValue) const;
    void ThrowOnError(TError error) const;
    // Feeds the arguments to TError, enriches it with path info and throws.
    template <typename... Args>
    [[noreturn]] void Throw(Args&&... args) const;
};

/// Construction kit for pain-free (hopefully) protobuf traversals.
//
// 1. Make your own visitor by subclassing a suitable specialization of TProtoVisitor. Make it final
// to inline virtuals. The template parameter is the fully qualified (with const, ref, or pointer)
// message wrapper (pointer or container of pointers) supplied to all methods. See traits for actual
// specializations. Or write your own.
//
// 2. Set policy flags of TProtoVisitorBase in the constructor or at call site.
//
// 3. Override the methods that handle structures that are relevant to your task. As a general
// pattern, try to handle a situation (say, PathComplete says you've reached your destination) and
// fall back to the base implementation. Note that the visitor is recursive, so not calling the base
// stops the visit of the current subtree.
//
// 4. Feel free to use utilities in the base. In particular, Throw() enriches errors with path
// information, which is always available in Tokenizer_/CurrentPath_.
//
// Tokenizer_ is always advanced after a token is converted into something that will be passed into
// the next method (the next message, an index or a key). Expect every method to be at the next /token
// or at PathComplete.
//
// 5. Call Visit with the (wrapped) message and the path you want to examine.

/// Design notes.
//
// The visitor is recursive because the language will manage a DFS stack much better than us.
// The nanoseconds are well-spent on virtual functions since it's convenient to wrap custom behavior
// around them.
//
// The methods try to be obvious, but you'll probably end up examining the base implementation to
// see what is and is not done for you. You probably want to at least handle terminals wherever we
// Throw Unimplemented.
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
// The Visit method is separately templated to take things that it can get messages out of.
// Specifically, it can traverse vectors and maps as well as pointers and references to messages.
// This is separate from defining a vector as TWrappedMessage:
// - A bunch of messages in TWrappedMessage are distinct message trees that are traversed in
//   parallel. You receive these in the overrides and handle them appropriately. For example,
//   parallel traversal of a pair of messages is a natural way of implementing comparison.
// - A map or vector as the top-level container is the top-level node in one message tree. Its index
//   or key is the first entry in the path. For example, an ORM attribute may be modeled as a
//   repeated field of messages, stored in the database as a YSON list and represented in memory as
//   a vector of messages. The visitor will interpret an asterisk as a request to visit all entries
//   in the vector, ditto for an integer path entry.
// The use case is writing generic code in ORM attributes that calls Visit and just works.
// Currently, we don't support parallel traversal of containers of containers (e. g., a pair of
// vectors). It could be implemented if there's a use case.

template <typename TWrappedMessage>
class TProtoVisitor
    : public TProtoVisitorBase
{
public:
    using TTraits = TProtoVisitorTraits<TWrappedMessage>;

    using TMessageParam = typename TTraits::TMessageParam;
    using TMessageReturn = typename TTraits::TMessageReturn;

    template <typename TVisitParam>
    void Visit(TVisitParam&& target, NYPath::TYPathBuf path);

protected:
    /// Outer containers section.
    // The parameter is a message pointer or reference.
    template <typename TVisitParam>
    void VisitScalar(TVisitParam&& messageScalar, EVisitReason reason);

    // The parameter is a vector of message pointers or references.
    template <typename TVisitParam>
    void VisitVector(TVisitParam&& messageVector, EVisitReason reason);

    // Called for asterisks and visits after the path.
    template <typename TVisitParam>
    void VisitWholeVector(TVisitParam&& messageVector, EVisitReason reason);

    // The parameter is a map of message pointers or references.
    template <typename TVisitParam>
    void VisitMap(TVisitParam&& messageMap, EVisitReason reason);

    // Called for asterisks and visits after the path.
    template <typename TVisitParam>
    void VisitWholeMap(TVisitParam&& messageMap, EVisitReason reason);

    /// Message section.
    // Generic message dispatcher. Called for the initial message of the visit and every recursion.
    // Distinguishes special message types (currently, TAttributeDictionary).
    virtual void VisitMessage(TMessageParam message, EVisitReason reason);

    // Called for non-special message types.
    virtual void VisitRegularMessage(
        TMessageParam message,
        const NProtoBuf::Descriptor* descriptor,
        EVisitReason reason);

    // Called for asterisks and visits after the path.
    virtual void VisitWholeMessage(TMessageParam message, EVisitReason reason);
    // The field name with this name was not found in the message. Not to be confused with unknown
    // fields (although the field may be found in the unknown field set).
    // Current message descriptor and unknown field name provided for convenience.
    virtual void VisitUnrecognizedField(
        TMessageParam message,
        const NProtoBuf::Descriptor* descriptor,
        TString name,
        EVisitReason reason);
    // Called when there is a problem with looking up the message descriptor (e.g., mismatching
    // descriptors in a wrap or an empty wrap). Throws the error by default.
    virtual void OnDescriptorError(
        TMessageParam message,
        EVisitReason reason,
        TError error);

    /// AttributeDictionary section.
    // Called when ProcessAttributeDictionary is set and the message has the attribute_dictionary
    // option. The |fieldDescriptor| points to the repeated |attributes| field. Whole/entry/error
    // machinery is not provided; overrides should handle all appropriate situations.
    virtual void VisitAttributeDictionary(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    /// Generic field dispatcher. Calls map/repeated/singular variants.
    virtual void VisitField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    /// Map section.
    // Called for, well, map fields.
    virtual void VisitMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called for asterisks and visits after the path.
    virtual void VisitWholeMapField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // The entry was located. The specific paramerets are:
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
    virtual void VisitMapFieldEntry(
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
    virtual void OnKeyError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        std::unique_ptr<NProtoBuf::Message> keyMessage,
        TString key,
        EVisitReason reason,
        TError error);

    /// Repeated field section.
    // Called for, yes, repeated fields.
    virtual void VisitRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called for asterisks and visits after the path.
    virtual void VisitWholeRepeatedField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a specific entry in the repeated field. The index is within bounds.
    // Default implementation calls VisitMessage or throws.
    virtual void VisitRepeatedFieldEntry(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);
    // The path contained a relative index. The expected behavior is to insert a new entry *before*
    // the indexed one (so the new entry has the indicated index). The index is within bounds or
    // equals the repeated field size.
    virtual void VisitRepeatedFieldEntryRelative(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);
    // Called when there is a problem with evaluating field size (e.g., mismatching sizes in a
    // wrap). Throws the error by default.
    virtual void OnSizeError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error);
    // Called when there is a problem with evaluating field index (e.g., out of bounds).
    // Throws the error by default unless missing values are allowed.
    virtual void OnIndexError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error);

    /// Singular field section.
    // Called to visit a plain old singular field. Checks presence and calls
    // Visit[Present|Missing]SingularField or OnPresenceError.
    virtual void VisitSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a present singular field. Also called by default from VisitMapFieldEntry.
    // Default implementation calls VisitMessage or throws.
    virtual void VisitPresentSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called to visit a missing singular field. Throws unless convinced otherwise by flags and
    // reason.
    virtual void VisitMissingSingularField(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
    // Called when there is a problem with evaluating field precense (e.g., mismatching presence in
    // a wrap). Throws the error by default.
    virtual void OnPresenceError(
        TMessageParam message,
        const NProtoBuf::FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes

#define PROTO_VISITOR_INL_H_
#include "proto_visitor-inl.h"
#undef PROTO_VISITOR_INL_H_
