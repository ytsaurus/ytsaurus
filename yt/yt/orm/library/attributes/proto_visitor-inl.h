#ifndef PROTO_VISITOR_INL_H_
#error "Direct inclusion of this file is not allowed, include proto_visitor.h"
// For the sake of sane code completion.
#include "proto_visitor.h"
#endif

#include "helpers.h"
#include "proto_visitor_traits.h"

#include <yt/yt_proto/yt/core/ytree/proto/attributes.pb.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
TValue TProtoVisitorBase::ValueOrThrow(TErrorOr<TValue> errorOrValue) const
{
    if (errorOrValue.IsOK()) {
        return std::move(errorOrValue).Value();
    } else {
        Throw(errorOrValue);
    }
}

template <typename... Args>
[[noreturn]] void TProtoVisitorBase::Throw(Args&&... args) const
{
    THROW_ERROR TError(std::forward<Args>(args)...)
        << TErrorAttribute("path", Tokenizer_.GetPath())
        << TErrorAttribute("position", CurrentPath_.GetPath());
}

////////////////////////////////////////////////////////////////////////////////

template <typename TWrappedMessage>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage>::Visit(TVisitParam&& target, NYPath::TYPathBuf path)
{
    using TContainerTraits = TProtoVisitorContainerTraits<
        std::remove_cvref_t<TWrappedMessage>,
        std::remove_cvref_t<TVisitParam>>;

    Reset(path);

    if constexpr (TContainerTraits::IsScalar) {
        VisitScalar(std::forward<TVisitParam>(target), EVisitReason::TopLevel);
    } else if constexpr (TContainerTraits::IsVector) {
        VisitVector(std::forward<TVisitParam>(target), EVisitReason::TopLevel);
    } else if constexpr (TContainerTraits::IsMap) {
        VisitMap(std::forward<TVisitParam>(target), EVisitReason::TopLevel);
    } else {
        Throw(EErrorCode::Unimplemented,
            "Cannot visit type %v with a visitor that expects %v",
            TypeName<TVisitParam>(),
            TypeName<TWrappedMessage>());
    }
}

template <typename TWrappedMessage>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage>::VisitScalar(TVisitParam&& messageScalar, EVisitReason reason)
{
    using TContainerTraits = TProtoVisitorContainerTraits<
        std::remove_cvref_t<TWrappedMessage>,
        std::remove_cvref_t<TVisitParam>>;

    if constexpr (TContainerTraits::TakeAddress) {
        VisitMessage(&messageScalar, reason);
    } else {
        VisitMessage(messageScalar, reason);
    }
}

template <typename TWrappedMessage>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage>::VisitVector(TVisitParam&& messageVector, EVisitReason reason)
{
    if (PathComplete()) {
        if (VisitEverythingAfterPath_) {
            VisitWholeVector(std::forward<TVisitParam>(messageVector), EVisitReason::AfterPath);
            return;
        } else {
            Throw(EErrorCode::Unimplemented, "Cannot handle whole message vectors");
        }
    }

    SkipSlash();

    if (Tokenizer_.GetType() == NYPath::ETokenType::Asterisk) {
        AdvanceOverAsterisk();
        VisitWholeVector(std::forward<TVisitParam>(messageVector), EVisitReason::Asterisk);
    } else {
        int size = messageVector.size();
        auto errorOrIndexParseResult = ParseCurrentListIndex(size);
        if (!errorOrIndexParseResult.IsOK()) {
            OnIndexError(
                nullptr,
                nullptr,
                reason,
                std::move(errorOrIndexParseResult));
            return;
        }
        auto& indexParseResult = errorOrIndexParseResult.Value();
        AdvanceOver(int(indexParseResult.Index));
        switch (indexParseResult.IndexType) {
            case EListIndexType::Absolute:
                VisitScalar(messageVector[indexParseResult.Index], EVisitReason::Path);
                break;
        case EListIndexType::Relative:
            Throw(EErrorCode::MalformedPath,
                "Unexpected relative path specifier %v",
                Tokenizer_.GetToken());
            break;
        default:
            YT_ABORT();
        }
    }
}

template <typename TWrappedMessage>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage>::VisitWholeVector(
    TVisitParam&& messageVector,
    EVisitReason reason)
{
    int size = messageVector.size();
    for (int index = 0; !StopIteration_ && index < size; ++index) {
        auto checkpoint = CheckpointBranchedTraversal(index);
        VisitScalar(messageVector[index], reason);
    }
}

template <typename TWrappedMessage>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage>::VisitMap(TVisitParam&& messageMap, EVisitReason reason)
{
    using TContainerTraits = TProtoVisitorContainerTraits<
        std::remove_cvref_t<TWrappedMessage>,
        std::remove_cvref_t<TVisitParam>>;

    if (PathComplete()) {
        if (VisitEverythingAfterPath_) {
            VisitWholeMap(std::forward<TVisitParam>(messageMap), EVisitReason::AfterPath);
            return;
        } else {
            Throw(EErrorCode::Unimplemented, "Cannot handle whole message maps");
        }
    }

    SkipSlash();

    if (Tokenizer_.GetType() == NYPath::ETokenType::Asterisk) {
        AdvanceOverAsterisk();
        VisitWholeMap(std::forward<TVisitParam>(messageMap), EVisitReason::Asterisk);
    } else {
        Expect(NYPath::ETokenType::Literal);

        TString key = Tokenizer_.GetLiteralValue();
        AdvanceOver(key);

        auto it = messageMap.end();
        if constexpr (std::is_same_v<TString, typename TContainerTraits::TMapKey>) {
            it = messageMap.find(key);
        } else {
            typename TContainerTraits::TMapKey castKey;
            if (!TryFromString(key, castKey)) {
                Throw(EErrorCode::MalformedPath, "Invalid map key %v", key);
            }
            it = messageMap.find(castKey);
        }

        if (it == messageMap.end()) {
            OnKeyError(
                nullptr,
                nullptr,
                nullptr,
                std::move(key),
                reason,
                TError(EErrorCode::MissingKey, "Key not found in map"));
            return;
        }

        VisitScalar(it->second, EVisitReason::Path);
    }
}

template <typename TWrappedMessage>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage>::VisitWholeMap(TVisitParam&& messageMap, EVisitReason reason)
{
    for (auto& [key, entry] : messageMap) {
        auto checkpoint = CheckpointBranchedTraversal(key);
        VisitScalar(entry, reason);
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitMessage(TMessageParam message, EVisitReason reason)
{
    auto errorOrDescriptor = TTraits::GetDescriptor(message);
    if (!errorOrDescriptor.IsOK()) {
        OnDescriptorError(message, reason, std::move(errorOrDescriptor));
        return;
    }
    const auto* descriptor = errorOrDescriptor.Value();

    if (ProcessAttributeDictionary_
        && descriptor->options().GetExtension(NYson::NProto::attribute_dictionary))
    {
        const auto* fieldDescriptor = descriptor->FindFieldByName("attributes");
        if (!fieldDescriptor) {
            Throw(EErrorCode::InvalidData, "Misplaced attribute_dictionary option");
        }
        VisitAttributeDictionary(message, fieldDescriptor, reason);
        return;
    }

    VisitRegularMessage(message, descriptor, reason);
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitRegularMessage(
    TMessageParam message,
    const NProtoBuf::Descriptor* descriptor,
    EVisitReason reason)
{
    if (PathComplete()) {
        if (VisitEverythingAfterPath_) {
            VisitWholeMessage(message, EVisitReason::AfterPath);
            return;
        } else {
            Throw(EErrorCode::Unimplemented, "Cannot handle whole message fields");
        }
    }

    SkipSlash();

    if (Tokenizer_.GetType() == NYPath::ETokenType::Asterisk) {
        AdvanceOverAsterisk();
        VisitWholeMessage(message, EVisitReason::Asterisk);
    } else {
        Expect(NYPath::ETokenType::Literal);

        TString name = Tokenizer_.GetLiteralValue();
        AdvanceOver(name);
        const auto* fieldDescriptor = descriptor->FindFieldByName(name);
        if (fieldDescriptor) {
            VisitField(message, fieldDescriptor, EVisitReason::Path);
        } else {
            VisitUnrecognizedField(message, descriptor, std::move(name), reason);
        }
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitWholeMessage(TMessageParam message, EVisitReason reason)
{
    auto errorOrDescriptor = TTraits::GetDescriptor(message);
    if (!errorOrDescriptor.IsOK()) {
        OnDescriptorError(message, reason, std::move(errorOrDescriptor));
        return;
    }
    const auto* descriptor = errorOrDescriptor.Value();

    for (int i = 0; !StopIteration_ && i < descriptor->field_count(); ++i) {
        auto* fieldDescriptor = descriptor->field(i);
        auto checkpoint = CheckpointBranchedTraversal(fieldDescriptor->name());
        VisitField(message, fieldDescriptor, reason);
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitUnrecognizedField(
    TMessageParam message,
    const NProtoBuf::Descriptor* descriptor,
    TString name,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(reason);

    if (!AllowMissing_) {
        Throw(EErrorCode::MissingField,
            "Field %v not found in message %v",
            name,
            descriptor->full_name());
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::OnDescriptorError(
    TMessageParam message,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(reason);

    Throw(std::move(error));
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitAttributeDictionary(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Throw(EErrorCode::Unimplemented, "Cannot handle whole attribute dictionaries");
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (fieldDescriptor->is_map()) {
        VisitMapField(message, fieldDescriptor, reason);
    } else if (fieldDescriptor->is_repeated()) {
        VisitRepeatedField(message, fieldDescriptor, reason);
    } else {
        VisitSingularField(message, fieldDescriptor, reason);
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitMapField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(reason);

    if (PathComplete()) {
        if (VisitEverythingAfterPath_) {
            VisitWholeMapField(message, fieldDescriptor, EVisitReason::AfterPath);
            return;
        } else {
            Throw(EErrorCode::Unimplemented, "Cannot handle whole map fields");
        }
    }

    SkipSlash();

    if (Tokenizer_.GetType() == NYPath::ETokenType::Asterisk) {
        AdvanceOverAsterisk();
        VisitWholeMapField(message, fieldDescriptor, EVisitReason::Asterisk);
    } else {
        Expect(NYPath::ETokenType::Literal);

        TString key = Tokenizer_.GetLiteralValue();
        AdvanceOver(key);

        auto keyMessage = MakeMapKeyMessage(fieldDescriptor, key);
        auto errorOrEntry = TTraits::GetMessageFromMapFieldEntry(
            message,
            fieldDescriptor,
            keyMessage.get());
        if (!errorOrEntry.IsOK()) {
            OnKeyError(
                message,
                fieldDescriptor,
                std::move(keyMessage),
                std::move(key),
                EVisitReason::Path,
                std::move(errorOrEntry));
            return;
        }

        VisitMapFieldEntry(
            message,
            fieldDescriptor,
            std::move(errorOrEntry).Value(),
            std::move(key),
            EVisitReason::Path);
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitWholeMapField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto errorOrEntries = TTraits::GetMessagesFromWholeMapField(message, fieldDescriptor);
    if (!errorOrEntries.IsOK()) {
        OnKeyError(
            message,
            fieldDescriptor,
            {},
            {},
            reason,
            std::move(errorOrEntries));
        return;
    }
    auto& entries = errorOrEntries.Value();

    for (auto& [key, entry] : entries) {
        auto checkpoint = CheckpointBranchedTraversal(key);
        VisitMapFieldEntry(message, fieldDescriptor, entry, key, reason);
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitMapFieldEntry(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    TMessageParam entryMessage,
    TString key,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(key);

    auto* valueFieldDescriptor = fieldDescriptor->message_type()->map_value();
    VisitSingularField(entryMessage, valueFieldDescriptor, reason);
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::OnKeyError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    std::unique_ptr<NProtoBuf::Message> keyMessage,
    TString key,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(keyMessage);
    Y_UNUSED(key);
    Y_UNUSED(reason);

    if (AllowMissing_ && error.GetCode() == EErrorCode::MissingKey) {
        return;
    }

    Throw(std::move(error));
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitRepeatedField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(reason);

    if (PathComplete()) {
        if (VisitEverythingAfterPath_) {
            VisitWholeRepeatedField(message, fieldDescriptor, EVisitReason::AfterPath);
            return;
        } else {
            Throw(EErrorCode::Unimplemented, "Cannot handle whole repeated fields");
        }
    }

    SkipSlash();

    if (Tokenizer_.GetType() == NYPath::ETokenType::Asterisk) {
        AdvanceOverAsterisk();
        VisitWholeRepeatedField(message, fieldDescriptor, EVisitReason::Asterisk);
    } else {
        auto errorOrSize = TTraits::GetRepeatedFieldSize(message, fieldDescriptor);
        if (!errorOrSize.IsOK()) {
            OnSizeError(message, fieldDescriptor, reason, std::move(errorOrSize));
            return;
        }
        auto errorOrIndexParseResult = ParseCurrentListIndex(errorOrSize.Value());
        if (!errorOrIndexParseResult.IsOK()) {
            OnIndexError(message, fieldDescriptor, reason, std::move(errorOrIndexParseResult));
            return;
        }
        auto& indexParseResult = errorOrIndexParseResult.Value();
        AdvanceOver(int(indexParseResult.Index));

        switch (indexParseResult.IndexType) {
            case EListIndexType::Absolute:
                VisitRepeatedFieldEntry(
                    message,
                    fieldDescriptor,
                    indexParseResult.Index,
                    EVisitReason::Path);
                break;
            case EListIndexType::Relative:
                VisitRepeatedFieldEntryRelative(
                    message,
                    fieldDescriptor,
                    indexParseResult.Index,
                    EVisitReason::Path);
                break;
            default:
                YT_ABORT();
        }
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitWholeRepeatedField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto errorOrSize = TTraits::GetRepeatedFieldSize(message, fieldDescriptor);
    if (!errorOrSize.IsOK()) {
        OnSizeError(message, fieldDescriptor, reason, std::move(errorOrSize));
        return;
    }

    for (int index = 0; !StopIteration_ && index < errorOrSize.Value(); ++index) {
        auto checkpoint = CheckpointBranchedTraversal(index);
        VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitRepeatedFieldEntry(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
        TMessageReturn next =
            TTraits::GetMessageFromRepeatedField(message, fieldDescriptor, index);
        VisitMessage(next, reason);
    } else if (!PathComplete()) {
        Throw(EErrorCode::MalformedPath,
            "Expected field %v to be a protobuf message, but got %v",
            fieldDescriptor->full_name(),
            fieldDescriptor->type_name());
    } else {
        Throw(EErrorCode::Unimplemented, "Cannot handle repeated scalar fields");
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitRepeatedFieldEntryRelative(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(index);
    Y_UNUSED(reason);

    Throw(EErrorCode::MalformedPath,
        "Unexpected relative path specifier %v",
        Tokenizer_.GetToken());
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::OnSizeError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Throw(std::move(error));
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::OnIndexError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    if (AllowMissing_ && error.GetCode() == EErrorCode::OutOfBounds) {
        return;
    }

    Throw(std::move(error));
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto errorOrPresent = TTraits::IsSingularFieldPresent(message, fieldDescriptor);
    if (!errorOrPresent.IsOK()) {
        OnPresenceError(message, fieldDescriptor, reason, std::move(errorOrPresent));
        return;
    }
    if (!errorOrPresent.Value()) {
        VisitMissingSingularField(message, fieldDescriptor, reason);
        return;
    }

    VisitPresentSingularField(message, fieldDescriptor, reason);
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitPresentSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
        TMessageReturn next =
            TTraits::GetMessageFromSingularField(message, fieldDescriptor);
        VisitMessage(next, reason);
    } else if (!PathComplete()) {
        Throw(EErrorCode::MalformedPath,
            "Expected field %v to be a protobuf message, but got %v",
            fieldDescriptor->full_name(),
            fieldDescriptor->type_name());
    } else {
        Throw(EErrorCode::Unimplemented, "Cannot handle singular scalar fields");
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitMissingSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(reason);

    if (!AllowMissing_ && reason == EVisitReason::Path) {
        Throw(EErrorCode::MissingField, "Missing field %v", fieldDescriptor->full_name());
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::OnPresenceError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Throw(std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
