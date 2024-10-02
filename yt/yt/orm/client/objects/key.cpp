#include "key.h"

#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NClient::NObjects {
namespace {

////////////////////////////////////////////////////////////////////////////////

// NB! Unversioned value must not outlive keyField variable.
NTableClient::TUnversionedValue KeyFieldToUnversionedValue(const TObjectKey::TKeyField& keyField)
{
    return Visit(keyField,
        [&] (i64 value) { return NTableClient::MakeUnversionedInt64Value(value); },
        [&] (ui64 value) { return NTableClient::MakeUnversionedUint64Value(value); },
        [&] (double value) { return NTableClient::MakeUnversionedDoubleValue(value); },
        [&] (bool value) { return NTableClient::MakeUnversionedBooleanValue(value); },
        [&] (const TString& value) { return NTableClient::MakeUnversionedStringValue(value); });
}

TFingerprint CalculateArcadiaUtilHash(const TObjectKey::TKeyFields& keyFields)
{
    THash<size_t> indexHasher;
    THash<TObjectKey::TKeyField::variant> fieldHasher;
    size_t result = 0;
    for (size_t i = 0; i < keyFields.size(); ++i) {
        result = CombineHashes(CombineHashes(result, indexHasher(i)), fieldHasher(keyFields[i]));
    }
    return result;
}

TFingerprint CalculateFarmHash(const TObjectKey::TKeyFields& keyFields)
{
    std::vector<NTableClient::TUnversionedValue> values;
    values.reserve(keyFields.size());
    std::transform(
        keyFields.begin(),
        keyFields.end(),
        std::back_inserter(values),
        KeyFieldToUnversionedValue);
    return NTableClient::GetFarmFingerprint(TRange(values));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

TObjectKey::TKeyField TObjectKey::TKeyField::GetZeroValue(NTableClient::EValueType type)
{
    switch (type) {
        case NQueryClient::EValueType::Int64:
            return TObjectKey::TKeyField(0);
        case NTableClient::EValueType::Uint64:
            return TObjectKey::TKeyField(0ull);
        case NTableClient::EValueType::Double:
            return TObjectKey::TKeyField(0.0);
        case NTableClient::EValueType::Boolean:
            return TObjectKey::TKeyField(false);
        case NTableClient::EValueType::String:
            return TObjectKey::TKeyField("");
        default:
            THROW_ERROR_EXCEPTION("Key fields of type %Qlv are not supported",
                type);
    }
}

bool TObjectKey::TKeyField::IsZero() const
{
    return Visit(*this,
        [&] (const auto& value) {
            return !static_cast<bool>(value);
        });
}

NQueryClient::NAst::TLiteralValue TObjectKey::TKeyField::AsLiteralValue() const
{
    return Visit(*this,
        [&] (const auto& value) {
            return NQueryClient::NAst::TLiteralValue(value);
        });
}

NQueryClient::EValueType TObjectKey::TKeyField::GetValueType() const
{
    return Visit(*this,
        [&] (i64) { return NQueryClient::EValueType::Int64; },
        [&] (ui64) { return NQueryClient::EValueType::Uint64; },
        [&] (double) { return NQueryClient::EValueType::Double; },
        [&] (bool) { return NQueryClient::EValueType::Boolean; },
        [&] (const TString&) { return NQueryClient::EValueType::String; });
}

////////////////////////////////////////////////////////////////////////////////

TObjectKey::TObjectKey(const TObjectKey& key)
    : KeyFields_(key.KeyFields_)
{ }

TObjectKey::TObjectKey(TObjectKey&& key)
    : KeyFields_(std::move(key.KeyFields_))
{ }

TObjectKey& TObjectKey::operator=(const TObjectKey& rhs)
{
    if (this != &rhs) {
        KeyFields_ = rhs.KeyFields_;
    }
    return *this;
}

TObjectKey& TObjectKey::operator=(TObjectKey&& rhs)
{
    if (this != &rhs) {
        KeyFields_ = std::move(rhs.KeyFields_);
    }
    return *this;
}

TObjectKey::TObjectKey(TKeyFields keyFields)
    : KeyFields_(keyFields)
{ }

const TObjectKey::TKeyFields& TObjectKey::AsKeyFields() const&
{
    return KeyFields_;
}

TObjectKey::TKeyFields TObjectKey::AsKeyFields() &&
{
    return std::move(KeyFields_);
}

TString TObjectKey::ToString() const
{
    TStringBuilder builder;
    TDelimitedStringBuilderWrapper wrapper(&builder, ";");
    for (const auto& keyField : KeyFields_) {
        wrapper->AppendFormat("%v", keyField);
    }
    return builder.Flush();
}

NQueryClient::NAst::TLiteralValueTuple TObjectKey::AsLiteralValueTuple() const
{
    NQueryClient::NAst::TLiteralValueTuple result;
    result.reserve(size());
    for (const auto& field : *this) {
        result.push_back(field.AsLiteralValue());
    }
    return result;
}

const TObjectKey::TKeyField& TObjectKey::operator[](size_t i) const
{
    return KeyFields_[i];
}

size_t TObjectKey::size() const
{
    return KeyFields_.size();
}

TObjectKey::operator bool() const
{
    return !KeyFields_.empty();
}

bool TObjectKey::operator==(const TObjectKey& rhs) const
{
    return KeyFields_ == rhs.KeyFields_;
}

bool TObjectKey::operator<(const TObjectKey& rhs) const
{
    return KeyFields_ < rhs.KeyFields_;
}

TFingerprint TObjectKey::CalculateHash(EHashType hashType) const
{
    switch (hashType) {
        case EHashType::ArcadiaUtil:
            return CalculateArcadiaUtilHash(KeyFields_);
        case EHashType::Farm:
            return CalculateFarmHash(KeyFields_);
    }
    YT_UNIMPLEMENTED();
}

TObjectKey::TKeyFields::const_iterator TObjectKey::begin() const
{
    return KeyFields_.begin();
}

TObjectKey::TKeyFields::const_iterator TObjectKey::end() const
{
    return KeyFields_.end();
}

TObjectKey TObjectKey::operator+(const TObjectKey& rhs) const
{
    TObjectKey result(*this);
    result.KeyFields_.insert(
        result.KeyFields_.end(),
        rhs.KeyFields_.begin(),
        rhs.KeyFields_.end());
    return result;
}

void FormatValue(TStringBuilderBase* builder, const TObjectKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("%v", key.ToString());
}

void FormatValue(TStringBuilderBase* builder, const TObjectKey::TKeyField& keyField, TStringBuf /*spec*/)
{
    Visit(keyField, [&] (const auto& value) { builder->AppendFormat("%v", value); });
}

void Serialize(const TObjectKey::TKeyField& keyField, NYson::IYsonConsumer* consumer)
{
    Visit(keyField,
        [&] (i64 value) { consumer->OnInt64Scalar(value); },
        [&] (ui64 value) { consumer->OnUint64Scalar(value); },
        [&] (double value) { consumer->OnDoubleScalar(value); },
        [&] (bool value) { consumer->OnBooleanScalar(value); },
        [&] (const TString& value) { consumer->OnStringScalar(value); });
}

void Deserialize(TObjectKey::TKeyField& keyField, const NYTree::INodePtr& node)
{
    switch (node->GetType()) {
        case NYTree::ENodeType::String:
            keyField.emplace<TString>(node->AsString()->GetValue());
            break;
        case NYTree::ENodeType::Int64:
            keyField.emplace<i64>(node->AsInt64()->GetValue());
            break;
        case NYTree::ENodeType::Uint64:
            keyField.emplace<ui64>(node->AsUint64()->GetValue());
            break;
        case NYTree::ENodeType::Double:
            keyField.emplace<double>(node->AsDouble()->GetValue());
            break;
        case NYTree::ENodeType::Boolean:
            keyField.emplace<bool>(node->AsBoolean()->GetValue());
            break;
        default:
            THROW_ERROR_EXCEPTION(
                "Unexpected YSON item type %Qlv when deserializing to key field",
                node->GetType());
            break;
    }
}

void Serialize(const TObjectKey& key, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .DoListFor(key, [] (NYTree::TFluentList list, const auto& keyField) {
            list.Item().Do([&] (auto fluent) {
                Serialize(keyField, fluent.GetConsumer());
            });
        });
}

void Deserialize(TObjectKey& key, const NYTree::INodePtr& node)
{
    auto listNode = node->AsList();
    int count = listNode->GetChildCount();
    TObjectKey::TKeyFields keyFields;
    keyFields.resize(count);

    for (int index = 0; index < count; ++index) {
        Deserialize(keyFields[index], listNode->GetChildOrThrow(index));
    }

    key = TObjectKey(std::move(keyFields));
}

////////////////////////////////////////////////////////////////////////////////

TObjectKey ParseKeyFromRow(const std::vector<TString>& keyColumns, const TNode& row)
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(keyColumns.size());

    for (const auto& column : keyColumns) {
        auto node = row[column];
        switch (node.GetType()) {
            case TNode::EType::Int64:
                keyFields.push_back(node.AsInt64());
                break;
            case TNode::EType::Uint64:
                keyFields.push_back(node.AsUint64());
                break;
            case TNode::EType::Double:
                keyFields.push_back(node.AsDouble());
                break;
            case TNode::EType::Bool:
                keyFields.push_back(node.AsBool());
                break;
            case TNode::EType::String:
                keyFields.push_back(node.AsString());
                break;
            default:
                THROW_ERROR_EXCEPTION("Unsupported key column value type %Qv in column %Qv",
                    node.GetType(),
                    column);
        }
    }

    return TObjectKey(std::move(keyFields));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects
