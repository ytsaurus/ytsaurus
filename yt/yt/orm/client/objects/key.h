#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/library/query/base/ast.h>

#include <library/cpp/yson/node/node.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <util/stream/output.h>

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TObjectKey
{
private:
    using TKeyFieldTypes = std::variant<i64, ui64, double, bool, TString>;

public:
    struct TKeyField
        : public TKeyFieldTypes
    {
        using TKeyFieldTypes::variant;
        TKeyField(float value)
            : TKeyFieldTypes(value)
        { }
        TKeyField(i32 value)
            : TKeyFieldTypes(value)
        { }
        TKeyField(ui32 value)
            : TKeyFieldTypes(static_cast<ui64>(value))
        { }

        static TKeyField GetZeroValue(NTableClient::EValueType type);

        bool IsZero() const;

        NQueryClient::NAst::TLiteralValue AsLiteralValue() const;
        NTableClient::EValueType GetValueType() const;
    };

    using TKeyFields = TCompactVector<TKeyField, 4>;

    TObjectKey() = default;
    TObjectKey(const TObjectKey& key);
    TObjectKey(TObjectKey&& key);
    TObjectKey& operator=(const TObjectKey& key);
    TObjectKey& operator=(TObjectKey&& key);
    explicit TObjectKey(TKeyFields keyFields);

    template<typename... Ts>
    explicit TObjectKey(Ts... keyFields);

    const TKeyFields& AsKeyFields() const&;
    TKeyFields AsKeyFields() &&;

    TString ToString() const;
    NQueryClient::NAst::TLiteralValueTuple AsLiteralValueTuple() const;

    size_t size() const;
    const TKeyField& operator[](size_t i) const;
    template<typename T>
    T GetWithDefault(size_t i, T defaultValue = {}) const;

    explicit operator bool() const;
    TFingerprint CalculateHash(EHashType type = EHashType::ArcadiaUtil) const;
    bool operator==(const TObjectKey& rhs) const;
    bool operator<(const TObjectKey& rhs) const;

    TKeyFields::const_iterator begin() const;
    TKeyFields::const_iterator end() const;

    TObjectKey operator+(const TObjectKey& rhs) const;

private:
    TKeyFields KeyFields_;
};

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TObjectKey& key, TStringBuf spec);
void FormatValue(TStringBuilderBase* builder, const TObjectKey::TKeyField& keyField, TStringBuf spec);

void Serialize(const TObjectKey::TKeyField& key, NYson::IYsonConsumer* consumer);
void Deserialize(TObjectKey::TKeyField& key, const NYTree::INodePtr& node);
void Serialize(const TObjectKey& key, NYson::IYsonConsumer* consumer);
void Deserialize(TObjectKey& key, const NYTree::INodePtr& node);

////////////////////////////////////////////////////////////////////////////////

TObjectKey ParseKeyFromRow(const std::vector<TString>& keyColumns, const TNode& row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

#define KEY_INL_H_
#include "key-inl.h"
#undef KEY_INL_H_
