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
using ::google::protobuf::Reflection;
using ::google::protobuf::UnknownField;
using ::google::protobuf::UnknownFieldSet;
using ::google::protobuf::internal::WireFormatLite;
using ::google::protobuf::io::CodedOutputStream;
using ::google::protobuf::io::StringOutputStream;
using ::google::protobuf::util::FieldComparator;
using ::google::protobuf::util::MessageDifferencer;

using namespace NYson;
using namespace NYTree;

constexpr int ProtobufMapKeyFieldNumber = 1;
constexpr int ProtobufMapValueFieldNumber = 2;

////////////////////////////////////////////////////////////////////////////////

// Returns [index of the item, its content].
TErrorOr<std::pair<int, TYsonString>> LookupUnknownYsonFieldsItem(
    UnknownFieldSet* unknownFields,
    TStringBuf key)
{
    int count = unknownFields->field_count();
    for (int index = 0; index < count; ++index) {
        auto* field = unknownFields->mutable_field(index);
        if (field->number() == UnknownYsonFieldNumber) {
            if (field->type() != UnknownField::TYPE_LENGTH_DELIMITED) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected type %v of item within yson unknown field set",
                    static_cast<int>(field->type()));
            }

            UnknownFieldSet tmpItem;
            if (!tmpItem.ParseFromString(field->length_delimited())) {
                return TError(NAttributes::EErrorCode::InvalidData, "Cannot parse UnknownYsonFields item");
            }

            if (tmpItem.field_count() != 2) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected field count %v in item within yson unknown field set",
                    tmpItem.field_count());
            }

            auto* keyField = tmpItem.mutable_field(0);
            auto* valueField = tmpItem.mutable_field(1);
            if (keyField->number() != ProtobufMapKeyFieldNumber) {
                std::swap(keyField, valueField);
            }

            if (keyField->number() != ProtobufMapKeyFieldNumber) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected key tag %v of item within yson unknown field set",
                    keyField->number());
            }
            if (keyField->type() != UnknownField::TYPE_LENGTH_DELIMITED) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected key type %v of item within yson unknown field set",
                    static_cast<int>(keyField->type()));
            }

            if (valueField->number() != ProtobufMapValueFieldNumber) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected value tag %v of item within yson unknown field set",
                    valueField->number());
            }
            if (valueField->type() != UnknownField::TYPE_LENGTH_DELIMITED) {
                return TError(NAttributes::EErrorCode::InvalidData,
                    "Unexpected value type %v of item within yson unknown field set",
                    static_cast<int>(valueField->type()));
            }

            if (keyField->length_delimited() == key) {
                return std::pair<int, TYsonString>{
                    index,
                    TYsonString(valueField->length_delimited())};
            }
        }
    }
    return TError(NAttributes::EErrorCode::MissingKey, "Unknown yson field not found");
}

TString SerializeUnknownYsonFieldsItem(TStringBuf key, TStringBuf value)
{
    TString output;
    StringOutputStream outputStream(&output);
    CodedOutputStream codedOutputStream(&outputStream);
    codedOutputStream.WriteTag(
        WireFormatLite::MakeTag(ProtobufMapKeyFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
    codedOutputStream.WriteVarint64(key.length());
    codedOutputStream.WriteRaw(key.data(), static_cast<int>(key.length()));
    codedOutputStream.WriteTag(
        WireFormatLite::MakeTag(ProtobufMapValueFieldNumber, WireFormatLite::WIRETYPE_LENGTH_DELIMITED));
    codedOutputStream.WriteVarint64(value.length());
    codedOutputStream.WriteRaw(value.data(), static_cast<int>(value.length()));
    return output;
}

////////////////////////////////////////////////////////////////////////////////

class TClearVisitor final
    : public TProtoVisitor<Message*, TClearVisitor>
{
    friend class TProtoVisitor<Message*, TClearVisitor>;

protected:
    void VisitWholeMessage(
        Message* message,
        EVisitReason reason)
    {
        if (PathComplete()) {
            // Asterisk means clear all fields but keep the message present.
            message->Clear();
            return;
        }

        TProtoVisitor::VisitWholeMessage(message, reason);
    }

    void VisitWholeMapField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            // User supplied a useless trailing asterisk. Avoid quadratic deletion.
            VisitField(message, fieldDescriptor, EVisitReason::Manual);
            return;
        }

        TProtoVisitor::VisitWholeMapField(message, fieldDescriptor, reason);
    }

    void VisitWholeRepeatedField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            // User supplied a useless trailing asterisk. Avoid quadratic deletion.
            VisitField(message, fieldDescriptor, EVisitReason::Manual);
            return;
        }

        TProtoVisitor::VisitWholeRepeatedField(message, fieldDescriptor, reason);
    }

    void VisitUnrecognizedField(
        Message* message,
        const Descriptor* descriptor,
        TString name,
        EVisitReason reason)
    {
        auto* unknownFields = message->GetReflection()->MutableUnknownFields(message);

        auto errorOrItem = LookupUnknownYsonFieldsItem(unknownFields, name);

        if (errorOrItem.IsOK()) {
            auto [index, value] = std::move(errorOrItem).Value();
            if (PathComplete()) {
                unknownFields->DeleteSubrange(index, 1);
                return;
            }

            auto root = value
                ? ConvertToNode(value)
                : GetEphemeralNodeFactory()->CreateMap();
            if (RemoveNodeByYPath(root, NYPath::TYPath{GetTokenizerInput()})) {
                value = ConvertToYsonString(root);
                auto* item = unknownFields->mutable_field(index)->mutable_length_delimited();
                *item = SerializeUnknownYsonFieldsItem(name, value.AsStringBuf());
                return;
            }
        }

        TProtoVisitor::VisitUnrecognizedField(message, descriptor, std::move(name), reason);
    }

    void VisitField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete()) {
            auto* reflection = message->GetReflection();
            if (!fieldDescriptor->has_presence() ||
                reflection->HasField(*message, fieldDescriptor))
            {
                reflection->ClearField(message, fieldDescriptor);
                return;
            } // Else let the basic implementation of MissingFieldPolicy do the check.
        }

        TProtoVisitor::VisitField(message, fieldDescriptor, reason);
    }

    void VisitMapFieldEntry(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        Message* entryMessage,
        TString key,
        EVisitReason reason)
    {
        if (PathComplete()) {
            int index = LocateMapEntry(message, fieldDescriptor, entryMessage).Value();
            DeleteRepeatedFieldEntry(message, fieldDescriptor, index);
            return;
        }

        TProtoVisitor::VisitMapFieldEntry(
            message,
            fieldDescriptor,
            entryMessage,
            std::move(key),
            reason);
    }

    void VisitRepeatedFieldEntry(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        if (PathComplete()) {
            DeleteRepeatedFieldEntry(message, fieldDescriptor, index);
            return;
        }

        TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }

    void DeleteRepeatedFieldEntry(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index)
    {
        auto* reflection = message->GetReflection();
        int size = reflection->FieldSize(*message, fieldDescriptor);
        for (++index; index < size; ++index) {
            reflection->SwapElements(message, fieldDescriptor, index - 1, index);
        }
        reflection->RemoveLast(message, fieldDescriptor);
    }
}; // TClearVisitor

////////////////////////////////////////////////////////////////////////////////

class TSetVisitor final
    : public TProtoVisitor<Message*, TSetVisitor>
{
    friend class TProtoVisitor<Message*, TSetVisitor>;

public:
    TSetVisitor(
        const INodePtr& value,
        const TProtobufWriterOptions& options,
        bool recursive)
        : CurrentValue_(value)
        , Options_(options)
        , Recursive_(recursive)
    {
        SetProcessAttributeDictionary(true);
    }

protected:
    INodePtr CurrentValue_;
    const TProtobufWriterOptions& Options_;
    const bool Recursive_;

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
                Throw(NAttributes::EErrorCode::Unimplemented,
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
            if (PathComplete() || Recursive_) {
                item = unknownFields->AddLengthDelimited(UnknownYsonFieldNumber);
            } else {
                Throw(errorOrItem);
            }
        } else {
            Throw(errorOrItem);
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
                    auto entry = ValueOrThrow(
                        AddAttributeDictionaryEntry(message, fieldDescriptor, key));
                    ThrowOnError(SetAttributeDictionaryEntryValue(
                        entry,
                        ConvertToYsonString(value)));
                }
            } else if (CurrentValue_->GetType() != NYTree::ENodeType::Entity) {
                Throw(NAttributes::EErrorCode::Unimplemented,
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
            value = ValueOrThrow(GetAttributeDictionaryEntryValue(entry));
        } else if (error.GetCode() == NAttributes::EErrorCode::MissingKey) {
            ThrowOnError(AddAttributeDictionaryEntry(message, fieldDescriptor, key));
            RotateLastEntryBeforeIndex(message, fieldDescriptor, index);
            entry = reflection->MutableRepeatedMessage(message, fieldDescriptor, index);
        } else {
            Throw(error);
        }

        StoreCurrentValueToYsonString(value);
        ThrowOnError(SetAttributeDictionaryEntryValue(entry, value));
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
                    auto keyMessage = ValueOrThrow(MakeMapKeyMessage(fieldDescriptor, key));
                    // The key is obviously missing from the cleared map. This will populate the
                    // entry.
                    OnKeyError(
                        message,
                        fieldDescriptor,
                        std::move(keyMessage),
                        key,
                        EVisitReason::Manual,
                        TError());
                }
            } else if (CurrentValue_->GetType() == NYTree::ENodeType::List) {
                // Falling back to a list of maps with explicit |key| and |value| fields.
                VisitRepeatedField(message, fieldDescriptor, reason);
            } else if (CurrentValue_->GetType() != NYTree::ENodeType::Entity) {
                Throw(NAttributes::EErrorCode::Unimplemented,
                    "Cannot set a proto map from a yson node of type %v",
                    CurrentValue_->GetType());
            }
            return;
        }

        TProtoVisitor::VisitMapField(message, fieldDescriptor, reason);
    }

    void OnKeyError(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        std::unique_ptr<Message> keyMessage,
        TString key,
        EVisitReason reason,
        TError error)
    {
        if (PathComplete() || Recursive_) {
            const auto* reflection = message->GetReflection();
            auto* entry = keyMessage.get();

            reflection->AddAllocatedMessage(message, fieldDescriptor, keyMessage.release());
            VisitMapFieldEntry(
                message,
                fieldDescriptor,
                entry,
                std::move(key),
                reason);

            return;
        }

        TProtoVisitor::OnKeyError(
            message,
            fieldDescriptor,
            std::move(keyMessage),
            std::move(key),
            reason,
            error);
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
                Throw(NAttributes::EErrorCode::Unimplemented,
                    "Cannot set a repeated proto field from a yson node of type %v",
                    CurrentValue_->GetType());
            }
            return;
        }

        TProtoVisitor::VisitRepeatedField(message, fieldDescriptor, reason);
    }

    void VisitRepeatedFieldEntry(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        if (PathComplete() && fieldDescriptor->cpp_type() != FieldDescriptor::CPPTYPE_MESSAGE) {
            ThrowOnError(
                SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, CurrentValue_));
            return;
        }

        TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
    }

    void VisitRepeatedFieldEntryRelative(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason)
    {
        if (PathComplete() || Recursive_) {
            if (fieldDescriptor->cpp_type() == FieldDescriptor::CPPTYPE_MESSAGE) {
                const auto* reflection = message->GetReflection();
                auto* entry = reflection->AddMessage(message, fieldDescriptor);
                VisitMessage(entry, EVisitReason::Manual);
            } else {
                ThrowOnError(
                    AddScalarRepeatedFieldEntry(message, fieldDescriptor, CurrentValue_));
            }

            RotateLastEntryBeforeIndex(message, fieldDescriptor, index);
            return;
        }

        TProtoVisitor::VisitRepeatedFieldEntryRelative(message, fieldDescriptor, index, reason);
    }

    void VisitSingularField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason)
    {
        if (PathComplete() || Recursive_) {
            if (fieldDescriptor->type() == NProtoBuf::FieldDescriptor::TYPE_MESSAGE) {
                // MutableMessage will create the field.
                TProtoVisitor::VisitPresentSingularField(
                    message,
                    fieldDescriptor,
                    EVisitReason::Manual);
            } else {
                ThrowOnError(SetScalarField(message, fieldDescriptor, CurrentValue_));
            }
            return;
        }

        TProtoVisitor::VisitSingularField(message, fieldDescriptor, reason);
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
                    Recursive_);
                value = ConvertToYsonString(root);
            } catch (std::exception& ex) {
                Throw(TError("Failed to store yson string") << ex);
            }
        }
    }

    std::vector<std::pair<TString, INodePtr>> SortedMapChildren() const
    {
        auto children = CurrentValue_->AsMap()->GetChildren();
        std::sort(
            children.begin(),
            children.end(),
            [] (const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
        return {children.begin(), children.end()};
    }

    // Put the new entry at |index| and slide everything forward. Makes a noop if index was pointing
    // after the last entry.
    static void RotateLastEntryBeforeIndex(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index)
    {
        const auto* reflection = message->GetReflection();
        int last = reflection->FieldSize(*message, fieldDescriptor) - 1;
        for (int pos = index; pos < last; ++pos) {
            reflection->SwapElements(message, fieldDescriptor, pos, last);
        }
    }
}; // TSetVisitor

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
                auto sizes = ValueOrThrow(TTraits::Combine(
                    TTraits::TSubTraits::GetRepeatedFieldSize(message.first, fieldDescriptor),
                    TTraits::TSubTraits::GetRepeatedFieldSize(message.second, fieldDescriptor),
                    NAttributes::EErrorCode::MismatchingSize));

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
                auto indexParseResults = ValueOrThrow(errorOrIndexParseResults);

                if (indexParseResults.first.IndexType != EListIndexType::Relative ||
                    indexParseResults.second.IndexType != EListIndexType::Relative)
                {
                    Throw(NAttributes::EErrorCode::MalformedPath,
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
    TSetVisitor visitor(value, options, recursive);
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
