#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/key.h>

#include <yt/yt/orm/library/attributes/proto_visitor.h>

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedRow ToUnversionedRow(
    const TObjectKey& key,
    const NTableClient::TRowBufferPtr& rowBuffer);

// A version of NTableClient::FromUnversionedRow.
// To parse a TObjectKey, pass a TObjectKey* followed by a size_t keySize.
template <class... Ts>
void FromUnversionedRow(const NTableClient::TUnversionedRow& row, Ts... values);

// Convert a range of versioned or unversioned values into TObjectKey.
template <class TValue>
inline TObjectKey ValuesToKey(const TRange<TValue>& values);

// Temporary glue for situations not ported to polymorphic keys.
// TODO(deep): remove.
TObjectId ExtractObjectIdFromObjectKey(const TObjectKey& key);

TObjectKey::TKeyField UnversionedValueToKeyField(
    const NTableClient::TUnversionedValue& unversionedValue);

TObjectKey GenerateObjectKey(const IObjectTypeHandler* typeHandler, TTransaction* transaction);

void ValidateObjectKey(
    const IObjectTypeHandler* typeHandler,
    const TObjectKey& key,
    const TString& overrideTitle = {});

TObjectKey::TKeyField ParseObjectKeyField(NYTree::INodePtr node, NTableClient::EValueType valueType);

TObjectKey::TKeyField ParseObjectKeyFieldFromSingularProtoField(
    const NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor);

TObjectKey::TKeyField ParseObjectKeyFieldFromRepeatedProtoField(
    const NProtoBuf::Message* message,
    const NProtoBuf::FieldDescriptor* fieldDescriptor,
    int index);

std::optional<TObjectKey::TKeyField> TryParseObjectKeyField(
    const NProtoBuf::Message* message,
    TStringBuf suffix,
    bool allowMissing = true,
    bool ignorePresence = false);

TObjectKey::TKeyField ParseObjectKeyField(
    const NProtoBuf::Message* message,
    TStringBuf suffix,
    bool ignorePresence = false);

// Does not clear result. Specialized for messages, vectors and maps of messages.
// Properly navigates asterisks, indices and keys in suffix.
template <typename TMessageWrap, typename TKeyFieldVector>
void ParseObjectKeyFields(
    TMessageWrap message,
    TStringBuf suffix,
    TKeyFieldVector& result);

TObjectKey ParseObjectKey(
    const TString& serializedKey,
    const std::vector<const TDBField*>& dbFields,
    bool allowPrefix = false);

TObjectKey ParseObjectKey(
    NYTree::IListNodePtr listNodePtr,
    const std::vector<const TDBField*>& dbFields,
    bool allowPrefix = false);

TObjectKey ParseObjectKey(const NProtoBuf::Message* message, const std::vector<TString>& suffixes);

// Does not clear result. Specialized for messages, vectors and maps of messages.
// Properly navigates asterisks, indices and keys in suffix.
template <typename TMessageWrap, typename TSuffixVector, typename TKeyVector>
void ParseObjectKeys(
    TMessageWrap* message,
    const TSuffixVector& suffixes,
    TKeyVector& result);

void StoreObjectKeyField(
    NProtoBuf::Message* message,
    TStringBuf suffix,
    TObjectKey::TKeyField keyField);

template <typename TMessageWrap, typename TKeyFieldVector>
void StoreObjectKeyFields(
    TMessageWrap* message,
    TStringBuf suffix,
    TKeyFieldVector keyFields);

void StoreObjectKey(
    NProtoBuf::Message* message,
    const std::vector<TString>& suffixes,
    const TObjectKey& key);

// Creates a key with type-specific zero values, which is usually the null key.
TObjectKey GenerateZeroKey(const std::vector<const TDBField*>& dbFields);

NQueryClient::NAst::TExpressionPtr BuildKeyExpression(
    const TDBFields& keyFields,
    const std::vector<const TScalarAttributeSchema*>& attributes,
    const TString& separator,
    IQueryContext* context);

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CConvertibleToKeyField = std::constructible_from<TObjectKey::TKeyField, T>;

template <class T>
concept CConvertibleToKeyFields = std::same_as<std::vector<i64>, T> ||
    std::same_as<std::vector<ui64>, T> ||
    std::same_as<std::vector<double>, T> ||
    std::same_as<std::vector<bool>, T> ||
    std::same_as<std::vector<TString>, T>;

constexpr bool IsKeyFieldType();

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using namespace NAttributes;
using namespace NProtoBuf;

class TKeyFieldReader final
    : public NAttributes::TProtoVisitor<const Message*, TKeyFieldReader>
{
    friend class NAttributes::TProtoVisitor<const Message*, TKeyFieldReader>;
    friend class NAttributes::TPathVisitor<TKeyFieldReader>;

public:
    using TCallback = std::function<void(TObjectKey::TKeyField)>;

    explicit TKeyFieldReader(TCallback callback);

    // The index of an an entry of a repeated field that is being traversed whole. Multiple nested
    // asterisks are not supported.
    ui64 GetIndex() const;

    DEFINE_BYVAL_RW_PROPERTY(bool, IgnorePresence, false);

protected:
    const TCallback Callback_;
    std::optional<ui64> Index_;

    TCheckpoint CheckpointBranchedTraversal(TToken token);

    void VisitRepeatedField(
        const Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    void VisitRepeatedFieldEntry(
        const Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);

    void VisitPresentSingularField(
        const Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    void VisitMissingSingularField(
        const Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason);
}; // TKeyFieldReader

////////////////////////////////////////////////////////////////////////////////

class TKeyFieldWriter final
    : public NAttributes::TProtoVisitor<Message*, TKeyFieldWriter>
{
    friend class NAttributes::TProtoVisitor<Message*, TKeyFieldWriter>;
    friend class NAttributes::TPathVisitor<TKeyFieldWriter>;

public:
    using TCallback = std::function<TErrorOr<TObjectKey::TKeyField>()>;

    explicit TKeyFieldWriter(TCallback callback);

protected:
    const TCallback Callback_;

    template <typename TVisitParam>
    void VisitOther(TVisitParam&& target, EVisitReason reason);

    template <typename TVisitParam>
    void VisitWholeVector(TVisitParam&& messageVector, EVisitReason reason);

    template <typename TVisitParam, typename TMapKey>
    void OnMapKeyError(TVisitParam&& messageMap, TMapKey mapKey, TString key, EVisitReason reason);

    void VisitWholeRepeatedField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    void VisitRepeatedFieldEntry(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        int index,
        EVisitReason reason);

    void VisitSingularField(
        Message* message,
        const FieldDescriptor* fieldDescriptor,
        EVisitReason reason);

    TObjectKey::TKeyField NextKeyField();

    std::optional<TObjectKey::TKeyField> MaybeNextKeyField();

    template <typename TValue>
    TValue Cast(TObjectKey::TKeyField keyField) const;
}; // TKeyFieldReader

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT::NOrm::NServer::NObjects

#define KEY_UTIL_INL_H_
#include "key_util-inl.h"
#undef KEY_UTIL_INL_H_
