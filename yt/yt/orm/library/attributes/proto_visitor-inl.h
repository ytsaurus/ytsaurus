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

template <typename TWrappedMessage, typename TSelf>
template <typename TVisitParam>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitGeneric(TVisitParam&& target, EVisitReason reason)
{
    if constexpr (std::is_convertible_v<TVisitParam, TMessageParam>) {
        Self()->VisitMessage(target, reason);
        return;
    }

    if constexpr (
        std::is_base_of_v<std::remove_pointer_t<TMessageParam>, std::remove_reference_t<TVisitParam>>)
    {
        Self()->VisitMessage(&target, reason);
        return;
    }

    TPathVisitor<TSelf>::VisitGeneric(std::forward<TVisitParam>(target), reason);
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitMessage(TMessageParam message, EVisitReason reason)
{
    auto errorOrDescriptor = TTraits::GetDescriptor(message);
    if (!errorOrDescriptor.IsOK()) {
        Self()->OnDescriptorError(message, reason, std::move(errorOrDescriptor));
        return;
    }
    const auto* descriptor = errorOrDescriptor.Value();

    if (ProcessAttributeDictionary_
        && descriptor->options().GetExtension(NYson::NProto::attribute_dictionary))
    {
        const auto* fieldDescriptor = descriptor->FindFieldByName("attributes");
        if (!fieldDescriptor) {
            Self()->Throw(NAttributes::EErrorCode::InvalidData, "Misplaced attribute_dictionary option");
        }
        Self()->VisitAttributeDictionary(message, fieldDescriptor, reason);
        return;
    }

    Self()->VisitRegularMessage(message, descriptor, reason);
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitRegularMessage(
    TMessageParam message,
    const NProtoBuf::Descriptor* descriptor,
    EVisitReason reason)
{
    if (Self()->PathComplete()) {
        if (Self()->GetVisitEverythingAfterPath()) {
            Self()->VisitWholeMessage(message, EVisitReason::AfterPath);
            return;
        } else {
            Self()->Throw(NAttributes::EErrorCode::Unimplemented, "Cannot handle whole message fields");
        }
    }

    Self()->SkipSlash();

    if (Self()->GetTokenizerType() == NYPath::ETokenType::Asterisk) {
        Self()->AdvanceOverAsterisk();
        Self()->VisitWholeMessage(message, EVisitReason::Asterisk);
    } else {
        Self()->Expect(NYPath::ETokenType::Literal);

        TString name = Self()->GetLiteralValue();
        Self()->AdvanceOver(name);
        const auto* fieldDescriptor = descriptor->FindFieldByName(name);
        if (fieldDescriptor) {
            Self()->VisitField(message, fieldDescriptor, EVisitReason::Path);
        } else {
            Self()->VisitUnrecognizedField(message, descriptor, std::move(name), reason);
        }
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitWholeMessage(TMessageParam message, EVisitReason reason)
{
    auto errorOrDescriptor = TTraits::GetDescriptor(message);
    if (!errorOrDescriptor.IsOK()) {
        Self()->OnDescriptorError(message, reason, std::move(errorOrDescriptor));
        return;
    }
    const auto* descriptor = errorOrDescriptor.Value();

    for (int i = 0; !Self()->StopIteration_ && i < descriptor->field_count(); ++i) {
        auto* fieldDescriptor = descriptor->field(i);
        auto checkpoint = Self()->CheckpointBranchedTraversal(fieldDescriptor->name());
        Self()->VisitField(message, fieldDescriptor, reason);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitUnrecognizedField(
    TMessageParam message,
    const NProtoBuf::Descriptor* descriptor,
    TString name,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(reason);

    if (Self()->MissingFieldPolicy_ != EMissingFieldPolicy::Skip) {
        Self()->Throw(NAttributes::EErrorCode::MissingField,
            "Field %v not found in message %v",
            name,
            descriptor->full_name());
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::OnDescriptorError(
    TMessageParam message,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(reason);

    Self()->Throw(std::move(error));
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitAttributeDictionary(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Self()->Throw(NAttributes::EErrorCode::Unimplemented, "Cannot handle whole attribute dictionaries");
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (fieldDescriptor->is_map()) {
        Self()->VisitMapField(message, fieldDescriptor, reason);
    } else if (fieldDescriptor->is_repeated()) {
        Self()->VisitRepeatedField(message, fieldDescriptor, reason);
    } else {
        Self()->VisitSingularField(message, fieldDescriptor, reason);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto errorOrPresent = TTraits::IsSingularFieldPresent(message, fieldDescriptor);
    if (!errorOrPresent.IsOK()) {
        Self()->OnPresenceError(message, fieldDescriptor, reason, std::move(errorOrPresent));
        return;
    }
    if (!errorOrPresent.Value()) {
        Self()->VisitMissingSingularField(message, fieldDescriptor, reason);
        return;
    }

    Self()->VisitPresentSingularField(message, fieldDescriptor, reason);
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitPresentSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
        TMessageReturn next =
            TTraits::GetMessageFromSingularField(message, fieldDescriptor);
        Self()->VisitMessage(next, reason);
    } else if (!Self()->PathComplete()) {
        Self()->Throw(NAttributes::EErrorCode::MalformedPath,
            "Expected field %v to be a protobuf message, but got %v",
            fieldDescriptor->full_name(),
            fieldDescriptor->type_name());
    } else {
        Self()->VisitScalarSingularField(message, fieldDescriptor, reason);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitScalarSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Self()->Throw(NAttributes::EErrorCode::Unimplemented, "Cannot handle singular scalar fields");
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitMissingSingularField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto throwIfReasonIsPath = [&] {
        if (reason == EVisitReason::Path) {
            Self()->Throw(NAttributes::EErrorCode::MissingField,
                "Missing field %v",
                fieldDescriptor->full_name());
        }
    };

    switch (Self()->MissingFieldPolicy_) {
        case EMissingFieldPolicy::Throw:
            throwIfReasonIsPath();
            return;
        case EMissingFieldPolicy::Skip:
            return;
        case EMissingFieldPolicy::ForceLeaf:
            if (!Self()->PathComplete()) {
                throwIfReasonIsPath();
                return;
            }
        [[fallthrough]];
        case EMissingFieldPolicy::Force:
            // This will visit the default value if const/create and visit one if mutable.
            Self()->VisitPresentSingularField(message, fieldDescriptor, reason);
            return;
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::OnPresenceError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Self()->Throw(std::move(error));
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitRepeatedField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(reason);

    if (Self()->PathComplete()) {
        if (Self()->VisitEverythingAfterPath_) {
            Self()->VisitWholeRepeatedField(message, fieldDescriptor, EVisitReason::AfterPath);
            return;
        } else {
            Self()->Throw(NAttributes::EErrorCode::Unimplemented, "Cannot handle whole repeated fields");
        }
    }

    Self()->SkipSlash();

    if (Self()->GetTokenizerType() == NYPath::ETokenType::Asterisk) {
        Self()->AdvanceOverAsterisk();
        Self()->VisitWholeRepeatedField(message, fieldDescriptor, EVisitReason::Asterisk);
    } else {
        auto errorOrSize = TTraits::GetRepeatedFieldSize(message, fieldDescriptor);
        if (!errorOrSize.IsOK()) {
            Self()->OnSizeError(message, fieldDescriptor, reason, std::move(errorOrSize));
            return;
        }
        auto errorOrIndexParseResult = Self()->ParseCurrentListIndex(errorOrSize.Value());
        if (!errorOrIndexParseResult.IsOK()) {
            Self()->OnIndexError(
                message,
                fieldDescriptor,
                reason,
                std::move(errorOrIndexParseResult));
            return;
        }
        auto& indexParseResult = errorOrIndexParseResult.Value();
        Self()->AdvanceOver(indexParseResult.Index);

        switch (indexParseResult.IndexType) {
            case EListIndexType::Absolute:
                Self()->VisitRepeatedFieldEntry(
                    message,
                    fieldDescriptor,
                    indexParseResult.Index,
                    EVisitReason::Path);
                break;
            case EListIndexType::Relative:
                Self()->VisitRepeatedFieldEntryRelative(
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

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitWholeRepeatedField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto errorOrSize = TTraits::GetRepeatedFieldSize(message, fieldDescriptor);
    if (!errorOrSize.IsOK()) {
        Self()->OnSizeError(message, fieldDescriptor, reason, std::move(errorOrSize));
        return;
    }

    for (int index = 0; !StopIteration_ && index < errorOrSize.Value(); ++index) {
        auto checkpoint = Self()->CheckpointBranchedTraversal(index);
        Self()->VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitRepeatedFieldEntry(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
        TMessageReturn next =
            TTraits::GetMessageFromRepeatedField(message, fieldDescriptor, index);
        Self()->VisitMessage(next, reason);
    } else if (!PathComplete()) {
        Self()->Throw(NAttributes::EErrorCode::MalformedPath,
            "Expected field %v to be a protobuf message, but got %v",
            fieldDescriptor->full_name(),
            fieldDescriptor->type_name());
    } else {
        Self()->VisitScalarRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitScalarRepeatedFieldEntry(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(index);
    Y_UNUSED(reason);

    Self()->Throw(NAttributes::EErrorCode::Unimplemented, "Cannot handle repeated scalar fields");
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitRepeatedFieldEntryRelative(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    switch (Self()->MissingFieldPolicy_) {
        case EMissingFieldPolicy::Throw:
            break;
        case EMissingFieldPolicy::Skip:
            break; // Relative index means container modification.
        case EMissingFieldPolicy::ForceLeaf:
            if (!Self()->PathComplete()) {
                break;
            }
        [[fallthrough]];
        case EMissingFieldPolicy::Force:
            Self()->ThrowOnError(
                TTraits::InsertRepeatedFieldEntry(message, fieldDescriptor, index));
            Self()->VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
            return;
    }

    Self()->Throw(NAttributes::EErrorCode::MalformedPath,
        "Unexpected relative path specifier %v",
        GetToken());
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::OnSizeError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason,
    TError error)
{
    Y_UNUSED(message);
    Y_UNUSED(fieldDescriptor);
    Y_UNUSED(reason);

    Self()->Throw(std::move(error));
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::OnIndexError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason,
    TError error)
{
    if (error.GetCode() == NAttributes::EErrorCode::OutOfBounds) {
        switch (Self()->MissingFieldPolicy_) {
            case EMissingFieldPolicy::Throw:
                break;
            case EMissingFieldPolicy::Skip:
                return;
            case EMissingFieldPolicy::ForceLeaf:
                if (!PathComplete()) {
                    break;
                }
            [[fallthrough]];
            case EMissingFieldPolicy::Force:
                if constexpr (std::is_const_v<TMessageParam>) {
                    if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
                        TMessageReturn next =
                            TTraits::GetDefaultMessage(message, fieldDescriptor->message_type());
                        Self()->VisitMessage(next, reason);
                        return;
                    }
                }
                break; // We don't modify containers with bad indices.
        }
    }

    Self()->Throw(std::move(error));
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitMapField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    Y_UNUSED(reason);

    if (Self()->PathComplete()) {
        if (Self()->VisitEverythingAfterPath_) {
            Self()->VisitWholeMapField(message, fieldDescriptor, EVisitReason::AfterPath);
            return;
        } else {
            Self()->Throw(NAttributes::EErrorCode::Unimplemented, "Cannot handle whole map fields");
        }
    }

    Self()->SkipSlash();

    if (Self()->GetTokenizerType() == NYPath::ETokenType::Asterisk) {
        Self()->AdvanceOverAsterisk();
        Self()->VisitWholeMapField(message, fieldDescriptor, EVisitReason::Asterisk);
    } else {
        Self()->Expect(NYPath::ETokenType::Literal);

        TString key = Self()->GetLiteralValue();
        Self()->AdvanceOver(key);

        auto keyMessage = Self()->ValueOrThrow(MakeMapKeyMessage(fieldDescriptor, key));
        auto errorOrEntry = TTraits::GetMessageFromMapFieldEntry(
            message,
            fieldDescriptor,
            keyMessage.get());
        if (!errorOrEntry.IsOK()) {
            Self()->OnKeyError(
                message,
                fieldDescriptor,
                std::move(keyMessage),
                std::move(key),
                EVisitReason::Path,
                std::move(errorOrEntry));
            return;
        }

        Self()->VisitMapFieldEntry(
            message,
            fieldDescriptor,
            std::move(errorOrEntry).Value(),
            std::move(key),
            EVisitReason::Path);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitWholeMapField(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    auto errorOrEntries = TTraits::GetMessagesFromWholeMapField(message, fieldDescriptor);
    if (!errorOrEntries.IsOK()) {
        Self()->OnKeyError(
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
        auto checkpoint = Self()->CheckpointBranchedTraversal(key);
        Self()->VisitMapFieldEntry(message, fieldDescriptor, entry, key, reason);
    }
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::VisitMapFieldEntry(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    TMessageParam entryMessage,
    TString key,
    EVisitReason reason)
{
    Y_UNUSED(message);
    Y_UNUSED(key);

    auto* valueFieldDescriptor = fieldDescriptor->message_type()->map_value();
    Self()->VisitSingularField(entryMessage, valueFieldDescriptor, reason);
}

template <typename TWrappedMessage, typename TSelf>
void TProtoVisitor<TWrappedMessage, TSelf>::OnKeyError(
    TMessageParam message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    std::unique_ptr<NProtoBuf::Message> keyMessage,
    TString key,
    EVisitReason reason,
    TError error)
{
    if (error.GetCode() == NAttributes::EErrorCode::MissingKey) {
        switch (Self()->MissingFieldPolicy_) {
            case EMissingFieldPolicy::Throw:
                break;
            case EMissingFieldPolicy::Skip:
                return;
            case EMissingFieldPolicy::ForceLeaf:
                if (!PathComplete()) {
                    break;
                }
            [[fallthrough]];
            case EMissingFieldPolicy::Force:
                if constexpr (std::is_const_v<TMessageParam>) {
                    if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
                        TMessageReturn next =
                            TTraits::GetDefaultMessage(message, fieldDescriptor->message_type());
                        Self()->VisitMessage(next, reason);
                    } else {
                        Self()->Throw(
                            NAttributes::EErrorCode::Unimplemented,
                            "Cannot handle missing scalar map entries");
                    }
                } else {
                    auto entry = Self()->ValueOrThrow(
                        TTraits::InsertMapFieldEntry(
                            message,
                            fieldDescriptor,
                            std::move(keyMessage)));
                    Self()->VisitMapFieldEntry(
                        message,
                        fieldDescriptor,
                        std::move(entry),
                        std::move(key),
                        reason);
                }
                return;
        }
    }

    Self()->Throw(std::move(error));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
