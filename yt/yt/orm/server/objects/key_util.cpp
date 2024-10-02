#include "key_util.h"

#include "attribute_schema.h"
#include "object.h"
#include "persistence.h"
#include "type_handler.h"

#include <yt/yt/orm/client/objects/registry.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/query/base/helpers.h>

#include <library/cpp/iterator/zip.h>

#include <util/random/random.h>
#include <util/string/split.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NClient::NObjects;
using namespace NProtoBuf;
using namespace NTableClient;

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TKeyFieldReader::TKeyFieldReader(TCallback callback)
    : Callback_(std::move(callback))
{ }

ui64 TKeyFieldReader::GetIndex() const
{
    if (!Index_.has_value()) {
        Throw("Not at a location with a single index visited via an asterisk");
    }

    return Index_.value();
}

auto TKeyFieldReader::CheckpointBranchedTraversal(TToken token) -> TCheckpoint
{
    auto checkpoint = TProtoVisitor::CheckpointBranchedTraversal(token);

    ui64* index = std::get_if<ui64>(&token);
    if (index) {
        if (Index_.has_value()) {
            Throw(EErrorCode::Unimplemented,
                "Reading key fields with multiple dimensions is not supported");
        }
        Index_ = *index;
        checkpoint.Defer([this] () { Index_.reset(); });
    }

    return checkpoint;
}

void TKeyFieldReader::VisitRepeatedField(
    const Message* message,
    const FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (PathComplete() && AllowAsterisk_) {
        // COMPAT. Asking for a repeated field means asking for everything in it.
        VisitWholeRepeatedField(message, fieldDescriptor, EVisitReason::Manual);
        return;
    }

    TProtoVisitor::VisitRepeatedField(message, fieldDescriptor, reason);
}

void TKeyFieldReader::VisitRepeatedFieldEntry(
    const Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    if (PathComplete()) {
        try {
            Callback_(
                ParseObjectKeyFieldFromRepeatedProtoField(message, fieldDescriptor, index));
        } catch (std::exception& ex) {
            Throw(std::move(ex)); // Enrich with path info.
        }
        return;
    }

    TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
}

void TKeyFieldReader::VisitPresentSingularField(
    const Message* message,
    const FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (PathComplete()) {
        try {
            Callback_(
                ParseObjectKeyFieldFromSingularProtoField(message, fieldDescriptor));
        } catch (std::exception& ex) {
            Throw(std::move(ex)); // Enrich with path info.
        }

        return;
    }

    TProtoVisitor::VisitPresentSingularField(message, fieldDescriptor, reason);
}

void TKeyFieldReader::VisitMissingSingularField(
    const Message* message,
    const FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (IgnorePresence_) {
        // This will use the default value of the field.
        VisitPresentSingularField(message, fieldDescriptor, reason);
        return;
    }

    TProtoVisitor::VisitMissingSingularField(message, fieldDescriptor, reason);
}

////////////////////////////////////////////////////////////////////////////////

TKeyFieldWriter::TKeyFieldWriter(TCallback callback)
    : Callback_(std::move(callback))
{ }

void TKeyFieldWriter::VisitWholeRepeatedField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (PathComplete()) {
        auto* reflection = message->GetReflection();
        reflection->ClearField(message, fieldDescriptor);
        while (true) {
            auto maybeKeyField = MaybeNextKeyField();
            if (!maybeKeyField.has_value()) {
                return;
            }
            ThrowOnError(std::visit(
                [&] (auto&& value) {
                    return AddScalarRepeatedFieldEntry(message, fieldDescriptor, value);
                },
                std::move(maybeKeyField).value()));
        }
        return;
    }

    TProtoVisitor::VisitWholeRepeatedField(message, fieldDescriptor, reason);
}

void TKeyFieldWriter::VisitRepeatedFieldEntry(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index,
    EVisitReason reason)
{
    if (PathComplete()) {
        ThrowOnError(std::visit(
            [&] (auto&& value) {
                return SetScalarRepeatedFieldEntry(message, fieldDescriptor, index, value);
            },
            NextKeyField()));
        return;
    }

    TProtoVisitor::VisitRepeatedFieldEntry(message, fieldDescriptor, index, reason);
}

void TKeyFieldWriter::VisitSingularField(
    Message* message,
    const FieldDescriptor* fieldDescriptor,
    EVisitReason reason)
{
    if (PathComplete()) {
        ThrowOnError(std::visit(
            [&] (auto&& value) {
                return SetScalarField(message, fieldDescriptor, value);
            },
            NextKeyField()));
        return;
    }

    TProtoVisitor::VisitPresentSingularField(message, fieldDescriptor, reason);
}

TObjectKey::TKeyField TKeyFieldWriter::NextKeyField()
{
    return ValueOrThrow(Callback_());
}

std::optional<TObjectKey::TKeyField> TKeyFieldWriter::MaybeNextKeyField()
{
    auto errorOrKeyField = Callback_();

    if (errorOrKeyField.GetCode() == NAttributes::EErrorCode::Empty) {
        return std::nullopt;
    }

    return ValueOrThrow(errorOrKeyField);
}

////////////////////////////////////////////////////////////////////////////////

} //namespace

TUnversionedRow ToUnversionedRow(const TObjectKey& key, const TRowBufferPtr& rowBuffer)
{
    TUnversionedRowBuilder builder(key.size());

    for (size_t i = 0; i < key.size(); ++i) {
        Visit(key[i],
            [&] (i64 value) { builder.AddValue(MakeUnversionedInt64Value(value, i)); },
            [&] (ui64 value) { builder.AddValue(MakeUnversionedUint64Value(value, i)); },
            [&] (double value) { builder.AddValue(MakeUnversionedDoubleValue(value, i)); },
            [&] (bool value) { builder.AddValue(MakeUnversionedBooleanValue(value, i)); },
            [&] (const TString& value) { builder.AddValue(MakeUnversionedStringValue(value, i)); });
    }

    return rowBuffer->CaptureRow(builder.GetRow());
}

TObjectId ExtractObjectIdFromObjectKey(const TObjectKey& key)
{
    if (!key) {
        return "";
    }
    YT_VERIFY(key.size() == 1);
    YT_VERIFY(std::holds_alternative<TString>(key[0]));

    return std::get<TObjectId>(key[0]);
}

TObjectKey::TKeyField UnversionedValueToKeyField(
    const NTableClient::TUnversionedValue& unversionedValue)
{
    switch (unversionedValue.Type) {
        case NTableClient::EValueType::Int64:
            return NTableClient::FromUnversionedValue<i64>(unversionedValue);

        case NTableClient::EValueType::Uint64:
            return NTableClient::FromUnversionedValue<ui64>(unversionedValue);

        case NTableClient::EValueType::Double:
            return NTableClient::FromUnversionedValue<double>(unversionedValue);

        case NTableClient::EValueType::Boolean:
            return NTableClient::FromUnversionedValue<bool>(unversionedValue);

        case NTableClient::EValueType::String:
            return NTableClient::FromUnversionedValue<TString>(unversionedValue);

        default:
            THROW_ERROR_EXCEPTION("Unsupported value type %Qv", unversionedValue.Type);
    }
}

TObjectKey GenerateObjectKey(const IObjectTypeHandler* typeHandler, TTransaction* transaction)
{
    TObjectKey::TKeyFields fields;
    const auto& attributeSchemas = typeHandler->GetIdAttributeSchemas();
    const size_t count = attributeSchemas.size();
    fields.reserve(count);
    for (size_t i = 0; i != count; ++i) {
        fields.push_back(attributeSchemas[i]->GenerateKeyField(transaction));
    }
    return TObjectKey(std::move(fields));
}

void ValidateObjectKey(
    const IObjectTypeHandler* typeHandler,
    const TObjectKey& key,
    const TString& overrideTitle)
{
    THROW_ERROR_EXCEPTION_IF(key == typeHandler->GetNullKey(),
        NClient::EErrorCode::InvalidObjectId,
        "Key %Qv is the null key of %v",
        key.ToString(),
        NClient::NObjects::GetGlobalObjectTypeRegistry()->GetHumanReadableTypeNameOrCrash(
            typeHandler->GetType()));

    const auto& attributeSchemas = typeHandler->GetIdAttributeSchemas();
    const size_t count = attributeSchemas.size();
    YT_VERIFY(count == key.size());
    for (size_t i = 0; i != count; ++i) {
        attributeSchemas[i]->ValidateKeyField(key[i], overrideTitle);
    }
}

TObjectKey::TKeyField ParseObjectKeyField(
    NYTree::INodePtr node,
    NTableClient::EValueType valueType)
{
    if (node->GetType() == NYTree::ENodeType::Entity) {
        return TObjectKey::TKeyField::GetZeroValue(valueType);
    }
    // NB: These may involve type conversions.
    switch (valueType) {
        case EValueType::Int64:
            return node->GetValue<i64>();
        case EValueType::Uint64:
            return node->GetValue<ui64>();
        case EValueType::Double:
            return node->GetValue<double>();
        case EValueType::Boolean:
            return node->GetValue<bool>();
        case EValueType::String:
            return node->GetValue<TString>();
        default:
            THROW_ERROR_EXCEPTION("Cannot parse key field of type %Qlv",
                valueType);
    }
}

TObjectKey::TKeyField ParseObjectKeyFieldFromSingularProtoField(
    const Message* message,
    const FieldDescriptor* fieldDescriptor)
{
    YT_VERIFY(!fieldDescriptor->is_repeated());
    auto* reflection = message->GetReflection();
    switch (fieldDescriptor->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32:
            return reflection->GetInt32(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_UINT32:
            return reflection->GetUInt32(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_INT64:
            return reflection->GetInt64(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_UINT64:
            return reflection->GetUInt64(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return reflection->GetDouble(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return reflection->GetFloat(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_BOOL:
            return reflection->GetBool(*message, fieldDescriptor);
        case FieldDescriptor::CppType::CPPTYPE_ENUM:
        {
            const auto* enumValueDescriptor = reflection->GetEnum(*message, fieldDescriptor);
            if (enumValueDescriptor == nullptr) {
                break;
            }
            return EncodeEnumValue(enumValueDescriptor->name());
        }
        case FieldDescriptor::CppType::CPPTYPE_STRING:
            return reflection->GetString(*message, fieldDescriptor);
        default:
            break;
    }

    THROW_ERROR_EXCEPTION("Could not convert protobuf field of type %Qv to a key field",
        fieldDescriptor->type_name());
}

TObjectKey::TKeyField ParseObjectKeyFieldFromRepeatedProtoField(
    const Message* message,
    const FieldDescriptor* fieldDescriptor,
    int index)
{
    YT_VERIFY(fieldDescriptor->is_repeated());
    auto* reflection = message->GetReflection();
    switch (fieldDescriptor->cpp_type()) {
        case FieldDescriptor::CppType::CPPTYPE_INT32:
            return reflection->GetRepeatedInt32(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_UINT32:
            return reflection->GetRepeatedUInt32(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_INT64:
            return reflection->GetRepeatedInt64(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_UINT64:
            return reflection->GetRepeatedUInt64(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            return reflection->GetRepeatedDouble(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_FLOAT:
            return reflection->GetRepeatedFloat(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_BOOL:
            return reflection->GetRepeatedBool(*message, fieldDescriptor, index);
        case FieldDescriptor::CppType::CPPTYPE_ENUM:
        {
            const auto* enumValueDescriptor =
                reflection->GetRepeatedEnum(*message, fieldDescriptor, index);
            if (enumValueDescriptor == nullptr) {
                break;
            }
            return EncodeEnumValue(enumValueDescriptor->name());
        }
        case FieldDescriptor::CppType::CPPTYPE_STRING:
            return reflection->GetRepeatedString(*message, fieldDescriptor, index);
        default:
            break;
    }

    THROW_ERROR_EXCEPTION("Could not convert protobuf field of type %Qv to a key field",
        fieldDescriptor->type_name());
}

std::optional<TObjectKey::TKeyField> TryParseObjectKeyField(
    const Message* message,
    TStringBuf suffix,
    bool allowMissing,
    bool ignorePresence)
{
    std::optional<TObjectKey::TKeyField> result;
    NDetail::TKeyFieldReader reader(
        [&result] (TObjectKey::TKeyField keyField) {
            THROW_ERROR_EXCEPTION_IF(result.has_value(), "Key field was visited twice");
            result = std::move(keyField);
        });
    reader.SetAllowMissing(allowMissing);
    reader.SetIgnorePresence(ignorePresence);
    reader.Visit(message, suffix);
    return result;
}

TObjectKey::TKeyField ParseObjectKeyField(
    const Message* message,
    TStringBuf suffix,
    bool ignorePresence)
{
    auto optionalResult = TryParseObjectKeyField(
        message,
        suffix,
        /*allowMissing*/ false,
        /*ignorePresence*/ ignorePresence);
    THROW_ERROR_EXCEPTION_UNLESS(optionalResult.has_value(), "No key field at %v", suffix);
    return std::move(optionalResult).value();
}

TObjectKey ParseObjectKey(
    const TString& serializedKey,
    const std::vector<const TDBField*>& dbFields,
    bool allowPrefix)
{
    auto tokens = serializedKey.empty()
        ? std::vector<TStringBuf>{}
        : std::vector<TStringBuf>{StringSplitter(serializedKey).Split(CompositeKeySeparator)};
    size_t count = tokens.size();
    size_t expectedCount = dbFields.size();
    if (count > expectedCount) {
        THROW_ERROR_EXCEPTION("Key %Qv has more than expected %v fields",
            serializedKey,
            expectedCount);
    }
    if (!allowPrefix && count != 0 && count != expectedCount) {
        THROW_ERROR_EXCEPTION("Key %Qv does not have %v fields", serializedKey, expectedCount);
    }

    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const auto* dbField = dbFields[i];
        const auto& token = tokens[i];
        switch (dbField->Type) {
            case EValueType::Int64:
                keyFields.push_back(FromString<i64>(token));
                break;
            case EValueType::Uint64:
                keyFields.push_back(FromString<ui64>(token));
                break;
            case EValueType::Double:
                keyFields.push_back(FromString<double>(token));
                break;
            case EValueType::Boolean:
                keyFields.push_back(FromString<bool>(token));
                break;
            case EValueType::String:
                keyFields.push_back(ToString(token));
                break;
            default:
                THROW_ERROR_EXCEPTION("Parsing of key fields of type %Qv is not supported",
                    dbField->Type);
        }
    }
    return TObjectKey(std::move(keyFields));
}

TObjectKey ParseObjectKey(
    NYTree::IListNodePtr listNodePtr,
    const std::vector<const TDBField*>& dbFields,
    bool allowPrefix)
{
    size_t count = listNodePtr->GetChildCount();
    size_t expectedCount = dbFields.size();
    if (count > expectedCount) {
        THROW_ERROR_EXCEPTION("Key %Qv has more than expected %v fields",
            ConvertToYsonString(listNodePtr, NYson::EYsonFormat::Pretty),
            expectedCount);
    }
    if (!allowPrefix && count != 0 && count != expectedCount) {
        THROW_ERROR_EXCEPTION("Key %Qv does not have %v fields",
            ConvertToYsonString(listNodePtr, NYson::EYsonFormat::Pretty),
            expectedCount);
    }
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(count);
    for (size_t i = 0; i != count; ++i) {
        keyFields.push_back(ParseObjectKeyField(listNodePtr->FindChild(i), dbFields[i]->Type));
    }
    return TObjectKey(std::move(keyFields));
}

TObjectKey ParseObjectKey(const Message* message, const std::vector<TString>& suffixes)
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(suffixes.size());

    for (const auto& suffix : suffixes) {
        auto optionalKeyField = TryParseObjectKeyField(
            message,
            suffix,
            /*allowMissing*/ true,
            /*ignorePresence*/ false);
        if (!optionalKeyField.has_value()) {
            keyFields.clear();
            break;
        }
        keyFields.push_back(std::move(optionalKeyField).value());
    }

    YT_VERIFY(keyFields.empty() || keyFields.size() == suffixes.size());
    return TObjectKey(std::move(keyFields));
}

void StoreObjectKeyField(
    Message* message,
    TStringBuf suffix,
    TObjectKey::TKeyField keyField)
{
    bool stored = false;
    NDetail::TKeyFieldWriter writer([keyField = std::move(keyField), &stored] ()
        -> TErrorOr<TObjectKey::TKeyField>
    {
        if (stored) {
            return TError("Key field stored twice");
        }
        stored = true;
        return std::move(keyField);
    });
    writer.Visit(message, suffix);
    THROW_ERROR_EXCEPTION_UNLESS(stored, "Failed to store key field");
}

void StoreObjectKey(
    Message* message,
    const std::vector<TString>& suffixes,
    const TObjectKey& key)
{
    YT_VERIFY(!key || key.size() == suffixes.size());

    for (const auto& [i, suffix] : Enumerate(suffixes)) {
        if (key) {
            StoreObjectKeyField(message, suffix, key[i]);
        } else {
            NAttributes::ClearProtobufFieldByPath(*message, suffix, /*skipMissing*/ true);
        }
    }
}

TObjectKey GenerateZeroKey(const std::vector<const TDBField*>& dbFields)
{
    size_t count = dbFields.size();
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        const auto* dbField = dbFields[i];
        keyFields.push_back(TObjectKey::TKeyField::GetZeroValue(dbField->Type));
    }
    return TObjectKey(std::move(keyFields));
}

NQueryClient::NAst::TExpressionPtr BuildKeyExpression(
    const TDBFields& keyFields,
    const std::vector<const TScalarAttributeSchema*>& attributes,
    const TString& separator,
    IQueryContext* context)
{
    NQueryClient::NAst::TExpressionPtr prevExpressions = nullptr;
    NQueryClient::NAst::TExpressionPtr currentKey = nullptr;

    if (keyFields.size() != attributes.size()) {
        THROW_ERROR_EXCEPTION("Id attributes and key fields have different sizes");
    }

    for (const auto& [key, attribute] : Zip(keyFields, attributes)) {
        currentKey = attribute->RunExpressionBuilder(
            context,
            /*path*/ "",
            EAttributeExpressionContext::Fetch);

        switch (key->Type) {
            case EValueType::String:
                break;
            case EValueType::Boolean:
                currentKey = CastBoolToString(std::move(currentKey), context);
                break;
            case EValueType::Uint64:
                currentKey = CastUint64ToString(std::move(currentKey), context);
                break;
            case EValueType::Int64:
            case EValueType::Double:
                currentKey = CastNumericToString(std::move(currentKey), context);
                break;
            default:
                THROW_ERROR_EXCEPTION("Unexpected type %Qlv of key field %Qv", key->Type, key->Name);
        }

        prevExpressions = NQueryClient::BuildConcatenationExpression(
            context,
            prevExpressions,
            currentKey,
            separator);
    }

    return prevExpressions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
