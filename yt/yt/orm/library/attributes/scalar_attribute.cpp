#include "scalar_attribute.h"

#include "helpers.h"
#include "proto_visitor.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/ypath/helpers.h>
#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/protobuf_interop.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/serialize.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <library/cpp/iterator/functools.h>
#include <library/cpp/yt/misc/cast.h>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>
#include <google/protobuf/unknown_field_set.h>
#include <google/protobuf/util/message_differencer.h>
#include <google/protobuf/wire_format.h>

#include <variant>

namespace NYT::NOrm::NAttributes {

namespace {

////////////////////////////////////////////////////////////////////////////////

using ::google::protobuf::Descriptor;
using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Message;
using ::google::protobuf::util::MessageDifferencer;

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class TSelf, class TValueType>
class TSetVisitorBase
    : public TProtoVisitor<Message*, TSelf>
{
    using TPathVisitor<TSelf>::Self;

public:
    TSetVisitorBase(bool recursive, const TValueType& value)
        : CurrentValue_(value)
    {
        Self()->SetMissingFieldPolicy(recursive ? EMissingFieldPolicy::Force : EMissingFieldPolicy::ForceLeaf);
        Self()->SetProcessAttributeDictionary(true);
    }

protected:
    TValueType CurrentValue_;

    void VisitScalarRepeatedFieldEntry(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        Y_UNUSED(reason);
        SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, CurrentValue_).ThrowOnError();
    }

    void VisitScalarSingularField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        Y_UNUSED(reason);

        const auto* reflection = message->GetReflection();
        reflection->ClearField(message, fieldDescriptor);
        SetScalarField(message, fieldDescriptor, CurrentValue_).ThrowOnError();
    }
};

template <class T>
class TSetVisitor;

template<>
class TSetVisitor<NYTree::INodePtr> final
    : public TSetVisitorBase<TSetVisitor<NYTree::INodePtr>, INodePtr>
{
    friend class TProtoVisitor<Message*, TSetVisitor<NYTree::INodePtr>>;

public:
    TSetVisitor(
        const INodePtr& value,
        const TProtobufWriterOptions& options,
        bool recursive)
        : TSetVisitorBase(recursive, value)
        , Options_(options)
    { }

protected:
    const TProtobufWriterOptions& Options_;

    void VisitRegularMessage(
        Message* message,
        const Descriptor* descriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            message->Clear();

            if (CurrentValue_->GetType() == NYTree::ENodeType::Map) {
                for (const auto& [key, value] : SortedMapChildren()) {
                    if (value->GetType() == ENodeType::Entity) {
                        continue;
                    }
                    auto checkpoint = CheckpointBranchedTraversal(key);
                    TemporarilySetCurrentValue(checkpoint, value);
                    const auto* fieldDescriptor = descriptor->FindFieldByName(key);
                    if (fieldDescriptor) {
                        VisitField(message, fieldDescriptor, EVisitReason::Manual);
                    } else {
                        VisitUnrecognizedField(message, descriptor, key, reason);
                    }
                }
            } else if (CurrentValue_->GetType() != NYTree::ENodeType::Entity) {
                THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::Unimplemented,
                    "Cannot set a message from a yson node of type %v",
                    CurrentValue_->GetType());
            }
            return;
        }

        TProtoVisitor::VisitRegularMessage(message, descriptor, reason);
    }

    void VisitUnrecognizedField(
        Message* message,
        const Descriptor* descriptor,
        TString name,
        EVisitReason reason)
    {
        switch (Options_.UnknownYsonFieldModeResolver(GetCurrentPath())) {
            case EUnknownYsonFieldsMode::Keep:
                KeepUnrecognizedField(message, descriptor, name);
                return;
            case EUnknownYsonFieldsMode::Skip:
                return;
            // Forward in an object type handler attribute leaf is interpreted as Fail.
            case EUnknownYsonFieldsMode::Forward:
            case EUnknownYsonFieldsMode::Fail:
            default:
                break;
        }

        if (descriptor->IsReservedName(name)) {
            return;
        }

        TProtoVisitor::VisitUnrecognizedField(message, descriptor, std::move(name), reason);
    }

    void KeepUnrecognizedField(Message* message, const Descriptor* descriptor, TString name)
    {
        Y_UNUSED(descriptor);

        const auto* reflection = message->GetReflection();

        auto* unknownFields = reflection->MutableUnknownFields(message);
        auto errorOrItem = LookupUnknownYsonFieldsItem(unknownFields, name);

        TYsonString value;
        TString* item = nullptr;
        if (errorOrItem.IsOK()) {
            int index;
            std::tie(index, value) = std::move(errorOrItem).Value();
            item = unknownFields->mutable_field(index)->mutable_length_delimited();
        } else if (errorOrItem.GetCode() == NAttributes::EErrorCode::MissingKey) {
            if (PathComplete() || GetMissingFieldPolicy() == EMissingFieldPolicy::Force) {
                item = unknownFields->AddLengthDelimited(UnknownYsonFieldNumber);
            } else {
                THROW_ERROR_EXCEPTION(errorOrItem);
            }
        } else {
            THROW_ERROR_EXCEPTION(errorOrItem);
        }

        StoreCurrentValueToYsonString(value);
        *item = SerializeUnknownYsonFieldsItem(name, value.AsStringBuf());
    }

    void VisitAttributeDictionary(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        Y_UNUSED(reason);

        const auto* reflection = message->GetReflection();

        if (PathComplete()) {
            reflection->ClearField(message, fieldDescriptor);

            if (CurrentValue_->GetType() == NYTree::ENodeType::Map) {
                for (const auto& [key, value] : SortedMapChildren()) {
                    auto checkpoint = CheckpointBranchedTraversal(key);
                    auto entry = AddAttributeDictionaryEntry(message, fieldDescriptor, key)
                        .ValueOrThrow();
                    SetAttributeDictionaryEntryValue(entry, ConvertToYsonString(value))
                        .ThrowOnError();
                }
            } else if (CurrentValue_->GetType() != NYTree::ENodeType::Entity) {
                THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::Unimplemented,
                    "Cannot set an attribute dictionary from a yson node of type %v",
                    CurrentValue_->GetType());
            }
            return;
        }

        SkipSlash();

        TString key = GetLiteralValue();
        AdvanceOver(key);

        auto [index, error] = FindAttributeDictionaryEntry(message, fieldDescriptor, key);

        Message* entry = nullptr;
        TYsonString value;
        if (error.IsOK()) {
            entry = reflection->MutableRepeatedMessage(message, fieldDescriptor, index);
            value = GetAttributeDictionaryEntryValue(entry).ValueOrThrow();
        } else if (error.GetCode() == NAttributes::EErrorCode::MissingKey) {
            AddAttributeDictionaryEntry(message, fieldDescriptor, key).ThrowOnError();
            RotateLastEntryBeforeIndex(message, fieldDescriptor, index);
            entry = reflection->MutableRepeatedMessage(message, fieldDescriptor, index);
        } else {
            THROW_ERROR_EXCEPTION(error);
        }

        StoreCurrentValueToYsonString(value);
        SetAttributeDictionaryEntryValue(entry, value).ThrowOnError();
    }

    void VisitMapField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            const auto* reflection = message->GetReflection();
            reflection->ClearField(message, fieldDescriptor);

            if (CurrentValue_->GetType() == NYTree::ENodeType::Map) {
                for (const auto& [key, value] : SortedMapChildren()) {
                    auto checkpoint = CheckpointBranchedTraversal(key);
                    TemporarilySetCurrentValue(checkpoint, value);
                    auto keyMessage = MakeMapKeyMessage(fieldDescriptor, key).ValueOrThrow();
                    // The key is obviously missing from the cleared map. This will populate the
                    // entry.
                    OnKeyError(
                        message,
                        fieldDescriptor,
                        std::move(keyMessage),
                        key,
                        EVisitReason::Manual,
                        // NB! Poison pill for map processing. Error code `MissingKey` is processed
                        // according to missing field policy. Error message is not provided intentionally.
                        TError(EErrorCode::MissingKey, ""));
                }
            } else if (CurrentValue_->GetType() == NYTree::ENodeType::List) {
                // Falling back to a list of maps with explicit |key| and |value| fields.
                VisitRepeatedField(message, fieldDescriptor, reason);
            } else if (CurrentValue_->GetType() != NYTree::ENodeType::Entity) {
                THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::Unimplemented,
                    "Cannot set a proto map from a yson node of type %v",
                    CurrentValue_->GetType());
            }
            return;
        }

        TProtoVisitor::VisitMapField(message, fieldDescriptor, reason);
    }

    void VisitRepeatedField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            const auto* reflection = message->GetReflection();
            reflection->ClearField(message, fieldDescriptor);

            if (CurrentValue_->GetType() == NYTree::ENodeType::List) {
                for (const auto& [index, value] : Enumerate(CurrentValue_->AsList()->GetChildren())) {
                    auto checkpoint = CheckpointBranchedTraversal(int(index));
                    TemporarilySetCurrentValue(checkpoint, value);
                    // This is a bunch of insertions at the end of the array. Index points at the
                    // end.
                    VisitRepeatedFieldEntryRelative(
                        message,
                        fieldDescriptor,
                        int(index),
                        EVisitReason::Manual);
                }
            } else if (CurrentValue_->GetType() != NYTree::ENodeType::Entity) {
                THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::Unimplemented,
                    "Cannot set a repeated proto field from a yson node of type %v",
                    CurrentValue_->GetType());
            }
            return;
        }

        TProtoVisitor::VisitRepeatedField(message, fieldDescriptor, reason);
    }

    void TemporarilySetCurrentValue(TCheckpoint& checkpoint, INodePtr value)
    {
        checkpoint.Defer([this, oldCurrentValue = std::move(CurrentValue_)] () {
            CurrentValue_ = std::move(oldCurrentValue);
        });
        CurrentValue_ = std::move(value);
    }

    void StoreCurrentValueToYsonString(TYsonString& value) const
    {
        if (PathComplete()) {
            value = ConvertToYsonString(CurrentValue_);
        } else {
            auto root = value
                ? ConvertToNode(value)
                : GetEphemeralNodeFactory()->CreateMap();
            try {
                SyncYPathSet(
                    root,
                    NYPath::TYPath{GetTokenizerInput()},
                    ConvertToYsonString(CurrentValue_),
                    GetMissingFieldPolicy() == EMissingFieldPolicy::Force);
                value = ConvertToYsonString(root);
            } catch (std::exception& ex) {
                THROW_ERROR_EXCEPTION(TError("Failed to store yson string") << ex);
            }
        }
    }

    std::vector<std::pair<TString, INodePtr>> SortedMapChildren() const
    {
        auto children = CurrentValue_->AsMap()->GetChildren();
        std::ranges::sort(children, std::less{}, &std::pair<std::string, INodePtr>::first);
        return {children.begin(), children.end()};
    }
}; // TSetVisitor<INodePtr>

////////////////////////////////////////////////////////////////////////////////

template <>
class TSetVisitor<TWireString> final
    : public TSetVisitorBase<TSetVisitor<TWireString>, TWireString>
{
    friend class TProtoVisitor<Message*, TSetVisitor<TWireString>>;

public:
    TSetVisitor(
        const TWireString& value,
        bool recursive)
        : TSetVisitorBase(recursive, value)
    { }

protected:
    void VisitRegularMessage(
        Message* message,
        const Descriptor* descriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            message->Clear();
            MergeMessageFrom(message, CurrentValue_);
            return;
        }

        TProtoVisitor::VisitRegularMessage(message, descriptor, reason);
    }

    void VisitAttributeDictionary(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        Y_UNUSED(reason);

        const auto* reflection = message->GetReflection();

        if (PathComplete()) {
            reflection->ClearField(message, fieldDescriptor);
            MergeMessageFrom(message, CurrentValue_);
            return;
        }

        THROW_ERROR_EXCEPTION(
            "Partial set of TAttributeDictionary could not be performed via wire format");
    }

    void VisitMapField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            const auto* reflection = message->GetReflection();
            reflection->ClearField(message, fieldDescriptor);
            for (auto wireStringPart : CurrentValue_) {
                if (!reflection->AddMessage(message, fieldDescriptor)->MergeFromString(wireStringPart.AsStringView())) {
                    THROW_ERROR_EXCEPTION(TError(NAttributes::EErrorCode::InvalidData,
                        "Cannot parse map key-value pair from wire representation"));
                }
            }
            return;
        }

        TProtoVisitor::VisitMapField(message, fieldDescriptor, reason);
    }

    void VisitRepeatedField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            const auto* reflection = message->GetReflection();
            reflection->ClearField(message, fieldDescriptor);
            for (auto wireStringPart : CurrentValue_) {
                AddScalarRepeatedFieldEntry(message, fieldDescriptor, wireStringPart).ThrowOnError();
            }
            return;
        }

        TProtoVisitor::VisitRepeatedField(message, fieldDescriptor, reason);
    }
}; // TSetVisitor<TWireString>

////////////////////////////////////////////////////////////////////////////////

class TComparisonVisitor final
    : public TProtoVisitor<const std::pair<const Message*, const Message*>&, TComparisonVisitor>
{
    friend class TProtoVisitor<const std::pair<const Message*, const Message*>&, TComparisonVisitor>;

public:
    TComparisonVisitor()
    {
        SetAllowAsterisk(true);
        SetVisitEverythingAfterPath(true);
    }

    DEFINE_BYVAL_RW_PROPERTY(bool, Equal, true);

protected:
    void NotEqual()
    {
        Equal_ = false;
        StopIteration_ = true;
    }

    void OnDescriptorError(
        const std::pair<const Message*, const Message*>& message,
        EVisitReason reason,
        TError error)
    {
        if (error.GetCode() == NAttributes::EErrorCode::Empty) {
            // Both messages are null.
            return;
        }

        TProtoVisitor::OnDescriptorError(message, reason, std::move(error));
    }

    void OnKeyError(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        std::unique_ptr<NProtoBuf::Message> keyMessage,
        TString key,
        EVisitReason reason,
        TError error)
    {
        if (error.GetCode() == NAttributes::EErrorCode::MissingKey) {
            // Both fields are equally missing.
            return;
        }
        if (error.GetCode() == NAttributes::EErrorCode::MismatchingKeys) {
            // One present, one missing.
            NotEqual();
            return;
        }

        TProtoVisitor::OnKeyError(
            message,
            fieldDescriptor,
            std::move(keyMessage),
            std::move(key),
            reason,
            std::move(error));
    }

    void VisitRepeatedFieldEntry(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        if (PathComplete()
            && fieldDescriptor->type() != NProtoBuf::FieldDescriptor::TYPE_MESSAGE)
        {
            if (CompareScalarRepeatedFieldEntries(
                message.first,
                fieldDescriptor,
                index,
                message.second,
                fieldDescriptor,
                index) != std::partial_ordering::equivalent)
            {
                NotEqual();
            }
            return;
        }

        TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }

    void OnSizeError(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error)
    {
        if (error.GetCode() == NAttributes::EErrorCode::MismatchingSize) {
            if (reason == EVisitReason::Path) {
                // The caller wants to pinpoint a specific entry in two arrays of different sizes...
                // let's try!
                auto sizes = TTraits::Combine(
                    TTraits::TSubTraits::GetRepeatedFieldSize(message.first, fieldDescriptor),
                    TTraits::TSubTraits::GetRepeatedFieldSize(message.second, fieldDescriptor),
                    NAttributes::EErrorCode::MismatchingSize).ValueOrThrow();

                // Negative index may result in different parsed values!
                auto errorOrIndexParseResults = TTraits::Combine(
                    ParseCurrentListIndex(sizes.first),
                    ParseCurrentListIndex(sizes.second),
                    NAttributes::EErrorCode::MismatchingSize);


                if (errorOrIndexParseResults.GetCode() == NAttributes::EErrorCode::MismatchingSize) {
                    // Probably just one is out of bounds.
                    NotEqual();
                    return;
                }
                if (errorOrIndexParseResults.GetCode() == NAttributes::EErrorCode::OutOfBounds) {
                    // Equally out of bounds.
                    return;
                }
                auto indexParseResults = errorOrIndexParseResults.ValueOrThrow();

                if (indexParseResults.first.IndexType != EListIndexType::Relative ||
                    indexParseResults.second.IndexType != EListIndexType::Relative)
                {
                    THROW_ERROR_EXCEPTION(NAttributes::EErrorCode::MalformedPath,
                        "Unexpected relative path specifier %v",
                        GetToken());
                }
                AdvanceOver(indexParseResults.first.Index);

                if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
                    auto next = TTraits::Combine(
                        TTraits::TSubTraits::GetMessageFromRepeatedField(
                            message.first,
                            fieldDescriptor,
                            indexParseResults.first.Index),
                        TTraits::TSubTraits::GetMessageFromRepeatedField(
                            message.second,
                            fieldDescriptor,
                            indexParseResults.second.Index));
                    VisitMessage(next, EVisitReason::Manual);
                } else {
                    if (CompareScalarRepeatedFieldEntries(
                            message.first,
                            fieldDescriptor,
                            indexParseResults.first.Index,
                            message.second,
                            fieldDescriptor,
                            indexParseResults.second.Index) != std::partial_ordering::equivalent)
                        {
                            NotEqual();
                        }
                }
            } else {
                // Not a specific path request and mismatching size... done.
                NotEqual();
            }

            return;
        }

        TProtoVisitor::OnSizeError(message, fieldDescriptor, reason, std::move(error));
    }

    void OnIndexError(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error)
    {
        if (error.GetCode() == NAttributes::EErrorCode::OutOfBounds) {
            // Equally misplaced path. Would have been a size error if it were a mismatch.
            return;
        }

        TProtoVisitor::OnIndexError(message, fieldDescriptor, reason, std::move(error));
    }

    void VisitPresentSingularField(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()
            && fieldDescriptor->type() != NProtoBuf::FieldDescriptor::TYPE_MESSAGE)
        {
            if (CompareScalarFields(
                message.first,
                fieldDescriptor,
                message.second,
                fieldDescriptor) != std::partial_ordering::equivalent)
            {
                NotEqual();
            }
            return;
        }

        TProtoVisitor::VisitPresentSingularField(message, fieldDescriptor, reason);
    }

    void VisitMissingSingularField(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        Y_UNUSED(message);
        Y_UNUSED(fieldDescriptor);
        Y_UNUSED(reason);

        // Both fields are equally missing.
    }

    void OnPresenceError(
        const std::pair<const Message*, const Message*>& message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason,
        TError error)
    {
        if (error.GetCode() == NAttributes::EErrorCode::MismatchingPresence) {
            if (!PathComplete()
                && fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE)
            {
                // Try to check that the actual field is absent in both messages.
                auto next = TTraits::Combine(
                    TTraits::TSubTraits::IsSingularFieldPresent(
                        message.first,
                        fieldDescriptor).Value()
                    ? TTraits::TSubTraits::GetMessageFromSingularField(
                        message.first,
                        fieldDescriptor)
                    : nullptr,
                    TTraits::TSubTraits::IsSingularFieldPresent(
                        message.second,
                        fieldDescriptor).Value()
                    ? TTraits::TSubTraits::GetMessageFromSingularField(
                        message.second,
                        fieldDescriptor)
                    : nullptr);
                    VisitMessage(next, EVisitReason::Manual);
            } else {
                // One present, one missing.
                NotEqual();
            }
            return;
        }

        TProtoVisitor::OnPresenceError(message, fieldDescriptor, reason, std::move(error));
    }
}; // TComparisonVisitor

////////////////////////////////////////////////////////////////////////////////

} // namespace

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

bool AreProtoMessagesEqual(
    const Message& lhs,
    const Message& rhs,
    MessageDifferencer* messageDifferencer)
{
    if (messageDifferencer) {
        return messageDifferencer->Compare(lhs, rhs);
    }
    return MessageDifferencer::Equals(lhs, rhs);
}

bool AreProtoMessagesEqualByPath(
    const Message& lhs,
    const Message& rhs,
    const NYPath::TYPath& path)
{
    TComparisonVisitor visitor;
    visitor.Visit(std::pair(&lhs, &rhs), path);
    return visitor.GetEqual();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

void ClearProtobufFieldByPath(
    Message& message,
    const NYPath::TYPath& path,
    bool skipMissing)
{
    if (path.empty()) {
        // Skip visitor machinery in the simple use case.
        message.Clear();
    } else {
        TClearVisitor visitor;
        if (skipMissing) {
            visitor.SetMissingFieldPolicy(EMissingFieldPolicy::Skip);
        }
        visitor.SetAllowAsterisk(true);
        visitor.Visit(&message, path);
    }
}

void SetProtobufFieldByPath(
    Message& message,
    const NYPath::TYPath& path,
    const INodePtr& value,
    const TProtobufWriterOptions& options,
    bool recursive)
{
    TSetVisitor<INodePtr> visitor(value, options, recursive);
    visitor.Visit(&message, path);
}

void SetProtobufFieldByPath(
    Message& message,
    const NYPath::TYPath& path,
    const TWireString& value,
    bool recursive)
{
    TSetVisitor<TWireString> visitor(value, recursive);
    visitor.Visit(&message, path);
}

////////////////////////////////////////////////////////////////////////////////

template <>
bool AreScalarAttributesEqualByPath(
    const NYson::TYsonString& lhs,
    const NYson::TYsonString& rhs,
    const NYPath::TYPath& path)
{
    if (path.empty()) {
        return lhs == rhs;
    } else {
        return NYTree::TryGetAny(lhs.AsStringBuf(), path) == NYTree::TryGetAny(rhs.AsStringBuf(), path);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
