#ifndef KEY_UTIL_INL_H_
#error "Direct inclusion of this file is not allowed, include key_util.h"
// For the sake of sane code completion.
#include "key_util.h"
#endif

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class... Ts>
void FromUnversionedRowHelper(
    const NTableClient::TUnversionedRow& row,
    size_t index,
    TObjectKey* key,
    size_t keySize,
    Ts... values);

template <class T, class... Ts>
void FromUnversionedRowHelper(
    const NTableClient::TUnversionedRow& row,
    size_t index,
    T value,
    Ts... values)
{
    YT_VERIFY(index < row.GetCount());
    FromUnversionedValue(value, row[index]);
    if constexpr(sizeof...(Ts) > 0) {
        FromUnversionedRowHelper(row, index + 1, values...);
    }
}

template <class... Ts>
void FromUnversionedRowHelper(
    const NTableClient::TUnversionedRow& row,
    size_t index,
    TObjectKey* key,
    size_t keySize,
    Ts... values)
{
    YT_VERIFY(index + keySize <= row.GetCount());
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(keySize);
    for (size_t i = index; i < index + keySize; ++i) {
        keyFields.push_back(UnversionedValueToKeyField(row[i]));
    }
    *key = TObjectKey(std::move(keyFields));
    if constexpr(sizeof...(Ts) > 0) {
        FromUnversionedRowHelper(row, index + keySize, values...);
    }
}

template <typename TVisitParam>
void TKeyFieldWriter::VisitOther(TVisitParam&& target, EVisitReason /*reason*/)
{
    target = Cast<std::remove_reference_t<TVisitParam>>(NextKeyField());
}

template <typename TVisitParam>
void TKeyFieldWriter::VisitWholeVector(TVisitParam&& messageVector, EVisitReason reason)
{
    if (PathComplete()) {
        messageVector.clear();
        while (true) {
            auto maybeKeyField = MaybeNextKeyField();
            if (!maybeKeyField.has_value()) {
                return;
            }
            messageVector.push_back(
                Cast<typename std::remove_reference_t<TVisitParam>::value_type>(
                    std::move(maybeKeyField).value()));
        }
        return;
    }

    TProtoVisitor::VisitWholeVector(std::forward<TVisitParam>(messageVector), reason);
}

template <typename TVisitParam, typename TMapKey>
void TKeyFieldWriter::OnMapKeyError(
    TVisitParam&& messageMap,
    TMapKey mapKey,
    TString key,
    EVisitReason reason)
{
    if (PathComplete()) {
        messageMap[mapKey] =
            Cast<typename std::remove_reference_t<TVisitParam>::mapped_type>(NextKeyField());
        return;
    }

    TProtoVisitor::OnMapKeyError(
        std::forward<TVisitParam>(messageMap),
        std::move(mapKey),
        std::move(key),
        reason);
}

template <typename TValue>
TValue TKeyFieldWriter::Cast(TObjectKey::TKeyField keyField) const
{
    return std::visit(
        [&] (auto&& value) -> TValue {
            if constexpr (std::is_constructible_v<TValue, decltype(value)>) {
                return TValue(std::move(value));
            } else {
                Throw(NAttributes::EErrorCode::InvalidData,
                    "Cannot convert %v to %v",
                    TypeName(value),
                    TypeName<TValue>());
            }
        },
        std::move(keyField));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class... Ts>
void FromUnversionedRow(const NTableClient::TUnversionedRow& row, Ts... values)
{
    NDetail::FromUnversionedRowHelper(row, 0, values...);
}

template <class TValue>
inline TObjectKey ValuesToKey(const TRange<TValue>& values)
{
    TObjectKey::TKeyFields keyFields;
    keyFields.reserve(values.size());
    for (auto& val : values) {
        keyFields.push_back(UnversionedValueToKeyField(val));
    }
    return TObjectKey(std::move(keyFields));
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMessageWrap, typename TKeyFieldVector>
void ParseObjectKeyFields(
    TMessageWrap message,
    TStringBuf suffix,
    TKeyFieldVector& result)
{
    NDetail::TKeyFieldReader reader(
        [&result] (TObjectKey::TKeyField keyField) {
            result.push_back(std::move(keyField));
        });
    reader.SetAllowMissing(true);
    reader.SetAllowAsterisk(true);
    reader.Visit(message, suffix);
}

template <typename TMessageWrap, typename TSuffixVector, typename TKeyVector>
void ParseObjectKeys(
    TMessageWrap* message,
    const TSuffixVector& suffixes,
    TKeyVector& result)
{
    std::vector<TObjectKey::TKeyFields> keyFieldsVec;
    size_t fieldIndex = 0;
    size_t keySize = suffixes.size();
    bool hasAsterisk = false;

    NDetail::TKeyFieldReader reader([&] (TObjectKey::TKeyField keyField) {
        size_t keyIndex = hasAsterisk ? reader.GetIndex() : 0;
        if (keyFieldsVec.size() <= keyIndex) {
            keyFieldsVec.resize(keyIndex + 1);
            keyFieldsVec[keyIndex].reserve(keySize);
        }

        auto& keyFields = keyFieldsVec[keyIndex];

        // Check all previous fields have been collected. The size must equal the incoming index.
        THROW_ERROR_EXCEPTION_IF(keyFields.size() > fieldIndex,
            "Key field was visited twice");

        if (keyFields.size() < fieldIndex) {
            // Missed a field; stop filling this key.
            return;
        }
        keyFields.push_back(std::move(keyField));
    });
    reader.SetAllowMissing(true);
    reader.SetAllowAsterisk(true);

    for (const auto& [i, suffix] : Enumerate(suffixes)) {
        fieldIndex = i;
        hasAsterisk = suffix.Contains("/*");
        if (hasAsterisk && suffix.StartsWith("/*")) {
            reader.Visit(*message, suffix);
        } else {
            for (auto& entry : *message) {
                reader.Visit(entry, suffix);
            }
        }

        result.reserve(result.size() + keyFieldsVec.size());
        for (auto& keyFields : keyFieldsVec) {
            if (keyFields.size() < keySize) {
                continue;
            }
            result.push_back(TObjectKey(std::move(keyFields)));
        }
    }
}

template <typename TMessageWrap, typename TKeyFieldVector>
void StoreObjectKeyFields(
    TMessageWrap* message,
    TStringBuf suffix,
    TKeyFieldVector keyFields)
{
    auto begin = keyFields.begin();
    auto end = keyFields.end();
    NDetail::TKeyFieldWriter writer([&begin, &end] () -> TErrorOr<TObjectKey::TKeyField> {
        if (begin == end) {
            return TError(NAttributes::EErrorCode::Empty, "Not enough key fields to store");
        }
        return *(begin++); // Postincrement!
    });
    writer.SetAllowMissing(true);
    writer.SetAllowAsterisk(true);
    writer.Visit(*message, suffix);
    THROW_ERROR_EXCEPTION_UNLESS(begin == end, "Failed to store all key fields");
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
constexpr bool IsKeyFieldType()
{
    // Could use std::disjunction...
    return std::is_same<i64, T>() ||
        std::is_same<ui64, T>() ||
        std::is_same<double, T>() ||
        std::is_same<bool, T>() ||
        std::is_same<TString, T>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
