#ifndef PROTO_VISITOR_INL_H_
#error "Direct inclusion of this file is not allowed, include proto_visitor.h"
// For the sake of sane code completion.
#include "proto_visitor.h"
#endif

#include "helpers.h"
#include "proto_visitor_traits.h"

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
TValue TProtoVisitorBase::ValueOrThrow(TErrorOr<TValue> value) const
{
    if (value.IsOK()) {
        return std::move(value).Value();
    } else {
        Throw(value);
    }
}

template <typename... Args>
[[noreturn]] void TProtoVisitorBase::Throw(Args&&... args) const
{
    THROW_ERROR TError(std::forward<Args>(args)...)
        << TErrorAttribute("path", Tokenizer_.GetPath())
        << TErrorAttribute("position", Tokenizer_.GetPrefixPlusToken())
        << TErrorAttribute("stack", Stack_.GetPath());
}

////////////////////////////////////////////////////////////////////////////////

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::Visit(TMessageParam message, NYPath::TYPathBuf path)
{
    Tokenizer_.Reset(path);
    Tokenizer_.Advance();
    Stack_.Reset();
    StopIteration_ = false;
    VisitMessage(message, EVisitReason::TopLevel);
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitMessage(TMessageParam message, EVisitReason reason)
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
        Tokenizer_.Advance();
        VisitWholeMessage(message, EVisitReason::Asterisk);
    } else {
        Expect(NYPath::ETokenType::Literal);

        auto errorOrDescriptor = TTraits::GetDescriptor(message);
        if (!errorOrDescriptor.IsOK()) {
            OnDescriptorError(message, reason, std::move(errorOrDescriptor));
            return;
        }
        const auto* descriptor = errorOrDescriptor.Value();

        const auto* fieldDescriptor = descriptor->FindFieldByName(Tokenizer_.GetLiteralValue());
        if (fieldDescriptor) {
            Tokenizer_.Advance();
            VisitField(message, fieldDescriptor, EVisitReason::Path);
        } else {
            TString name = Tokenizer_.GetLiteralValue();
            Tokenizer_.Advance();
            VisitUnrecognizedField(message, descriptor, std::move(name), reason);
        }
    }
}

template <typename TWrappedMessage>
void TProtoVisitor<TWrappedMessage>::VisitWholeMessage(TMessageParam message, EVisitReason reason)
{
    if (!AllowAsterisk_ && reason == EVisitReason::Asterisk) {
        Throw(EErrorCode::Unimplemented, "Cannot handle asterisks in message traversals");
    }

    auto errorOrDescriptor = TTraits::GetDescriptor(message);
    if (!errorOrDescriptor.IsOK()) {
        OnDescriptorError(message, reason, std::move(errorOrDescriptor));
        return;
    }
    const auto* descriptor = errorOrDescriptor.Value();

    for (int i = 0; !StopIteration_ && i < descriptor->field_count(); ++i) {
        NYPath::TTokenizer::TCheckpoint checkpoint(Tokenizer_);
        auto* fieldDescriptor = descriptor->field(i);
        Stack_.Push(fieldDescriptor->name());
        VisitField(message, fieldDescriptor, reason);
        Stack_.Pop();
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
        Tokenizer_.Advance();
        VisitWholeMapField(message, fieldDescriptor, EVisitReason::Asterisk);
    } else {
        Expect(NYPath::ETokenType::Literal);

        TString key = Tokenizer_.GetLiteralValue();
        Tokenizer_.Advance();

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
    if (!AllowAsterisk_ && reason == EVisitReason::Asterisk) {
        Throw(EErrorCode::Unimplemented, "Cannot handle asterisks in map traversals");
    }

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
        NYPath::TTokenizer::TCheckpoint checkpoint(Tokenizer_);
        Stack_.Push(key);
        VisitMapFieldEntry(message, fieldDescriptor, entry, key, reason);
        Stack_.Pop();
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
    VisitPresentSingularField(entryMessage, valueFieldDescriptor, reason);
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
        Tokenizer_.Advance();
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
        Tokenizer_.Advance();
        auto& indexParseResult = errorOrIndexParseResult.Value();
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
    if (!AllowAsterisk_ && reason == EVisitReason::Asterisk) {
        Throw(EErrorCode::Unimplemented, "Cannot handle asterisks in repeated field traversals");
    }

    auto errorOrSize = TTraits::GetRepeatedFieldSize(message, fieldDescriptor);
    if (!errorOrSize.IsOK()) {
        OnSizeError(message, fieldDescriptor, reason, std::move(errorOrSize));
        return;
    }

    for (int index = 0; !StopIteration_ && index < errorOrSize.Value(); ++index) {
        NYPath::TTokenizer::TCheckpoint checkpoint(Tokenizer_);
        Stack_.Push(index);
        VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
        Stack_.Pop();
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
