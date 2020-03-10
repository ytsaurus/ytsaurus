#pragma once

#include "logical_type.h"
#include "row_base.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/optional.h>
#include <yt/core/misc/property.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

#include <util/digest/multi.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESortOrder,
    (Ascending)
)

constexpr int PrimaryLockIndex = 0;

DEFINE_ENUM(ELockType,
    ((None)         (0))
    ((SharedWeak)   (1))
    ((SharedStrong) (2))
    ((Exclusive)    (3))
);

class TLockMask
{
public:
    explicit TLockMask(TLockBitmap value = 0);

    ELockType Get(int index) const;
    void Set(int index, ELockType lock);

    void Enrich(int columnCount);

    TLockMask& operator = (const TLockMask& other) = default;
    operator TLockBitmap() const;

    static constexpr int BitsPerType = 2;
    static constexpr TLockBitmap TypeMask = (1 << BitsPerType) - 1;
    static constexpr int MaxCount = 8 * sizeof(TLockBitmap) / BitsPerType;

private:
    TLockBitmap Data_;
};

static_assert(TEnumTraits<ELockType>::GetMaxValue() <= ELockType((1 << TLockMask::BitsPerType) - 1));

TLockMask MaxMask(TLockMask lhs, TLockMask rhs);

////////////////////////////////////////////////////////////////////////////////

class TColumnSchema
{
public:
    // Keep in sync with hasher below.
    DEFINE_BYREF_RO_PROPERTY(TString, Name);
    DEFINE_BYREF_RO_PROPERTY(TLogicalTypePtr, LogicalType, NullLogicalType);
    DEFINE_BYREF_RO_PROPERTY(std::optional<ESortOrder>, SortOrder);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Lock);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Expression);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Aggregate);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Group);

    DEFINE_BYREF_RO_PROPERTY(std::optional<ESimpleLogicalValueType>, SimplifiedLogicalType);
    DEFINE_BYREF_RO_PROPERTY(bool, Required);

public:
    TColumnSchema();
    TColumnSchema(
        const TString& name,
        EValueType type,
        std::optional<ESortOrder> SortOrder = std::nullopt);
    TColumnSchema(
        const TString& name,
        ESimpleLogicalValueType type,
        std::optional<ESortOrder> SortOrder = std::nullopt);

    TColumnSchema(
        const TString& name,
        TLogicalTypePtr type,
        std::optional<ESortOrder> SortOrder = std::nullopt);

    TColumnSchema(const TColumnSchema&) = default;
    TColumnSchema(TColumnSchema&&) = default;

    TColumnSchema& operator=(const TColumnSchema&) = default;
    TColumnSchema& operator=(TColumnSchema&&) = default;

    TColumnSchema& SetName(const TString& name);
    TColumnSchema& SetLogicalType(TLogicalTypePtr valueType);
    TColumnSchema& SetSortOrder(const std::optional<ESortOrder>& value);
    TColumnSchema& SetLock(const std::optional<TString>& value);
    TColumnSchema& SetExpression(const std::optional<TString>& value);
    TColumnSchema& SetAggregate(const std::optional<TString>& value);
    TColumnSchema& SetGroup(const std::optional<TString>& value);

    EValueType GetPhysicalType() const;

    i64 GetMemoryUsage() const;
};

void FormatValue(TStringBuilderBase* builder, const TColumnSchema& schema, TStringBuf spec);

void Serialize(const TColumnSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TColumnSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema);
void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

////////////////////////////////////////////////////////////////////////////////

class TTableSchema
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TColumnSchema>, Columns);
    //! Strict schema forbids columns not specified in the schema.
    DEFINE_BYVAL_RO_PROPERTY(bool, Strict);
    DEFINE_BYVAL_RO_PROPERTY(bool, UniqueKeys);
    DEFINE_BYVAL_RW_PROPERTY(ETableSchemaModification, SchemaModification);

    //! Constructs an empty non-strict schema.
    TTableSchema();

    //! Constructs a schema with given columns and strictness flag.
    //! No validation is performed.
    explicit TTableSchema(
        std::vector<TColumnSchema> columns,
        bool strict = true,
        bool uniqueKeys = false,
        ETableSchemaModification schemaModification = ETableSchemaModification::None);

    const TColumnSchema* FindColumn(TStringBuf name) const;
    const TColumnSchema& GetColumn(TStringBuf name) const;
    const TColumnSchema& GetColumnOrThrow(TStringBuf name) const;

    int GetColumnIndex(const TColumnSchema& column) const;
    int GetColumnIndex(TStringBuf name) const;
    int GetColumnIndexOrThrow(TStringBuf name) const;

    TTableSchema Filter(
        const TColumnFilter& columnFilter,
        bool discardSortOrder = false) const;
    TTableSchema Filter(
        const THashSet<TString>& columns,
        bool discardSortOrder = false) const;
    TTableSchema Filter(
        const std::optional<std::vector<TString>>& columns,
        bool discardSortOrder = false) const;

    bool HasComputedColumns() const;
    bool IsSorted() const;
    bool IsUniqueKeys() const;

    TKeyColumns GetKeyColumns() const;
    int GetColumnCount() const;
    int GetKeyColumnCount() const;
    int GetValueColumnCount() const;

    bool HasNontrivialSchemaModification() const;

    //! Constructs a non-strict schema from #keyColumns assigning all components EValueType::Any type.
    //! #keyColumns could be empty, in which case an empty non-strict schema is returned.
    //! The resulting schema is validated.
    static TTableSchema FromKeyColumns(const TKeyColumns& keyColumns);

    //! Returns schema with first `keyColumnCount' columns sorted in ascending order
    //! and other columns non-sorted.
    TTableSchema SetKeyColumnCount(int keyColumnCount) const;

    //! Returns schema with `UniqueKeys' set to given value.
    TTableSchema SetUniqueKeys(bool uniqueKeys) const;

    //! For sorted tables, return the current schema as-is.
    //! For ordered tables, prepends the current schema with |(tablet_index, row_index)| key columns.
    TTableSchema ToQuery() const;

    //! For sorted tables, return the current schema without computed columns.
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column
    //! but without |$timestamp| column, if any.
    TTableSchema ToWrite() const;

    //! For sorted tables, return the current schema
    //! For ordered tables, prepends the current schema with |(tablet_index)| key column
    TTableSchema WithTabletIndex() const;

    //! Only applies to sorted replicated tables.
    //! Returns the current schema as-is.
    TTableSchema ToVersionedWrite() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchema ToLookup() const;

    //! For sorted tables, returns the non-computed key columns.
    //! For ordered tables, returns an empty schema.
    TTableSchema ToDelete() const;

    //! Returns just the key columns.
    TTableSchema ToKeys() const;

    //! Returns the non-key columns.
    TTableSchema ToValues() const;

    //! Returns the schema with UniqueKeys set to |true|.
    TTableSchema ToUniqueKeys() const;

    //! Returns the schema with all column attributes unset expect Name, Type and Required.
    TTableSchema ToStrippedColumnAttributes() const;

    //! Returns the schema with all column attributes unset expect Name, Type, Required and SortOrder.
    TTableSchema ToSortedStrippedColumnAttributes() const;

    //! Returns (possibly reordered) schema sorted by column names.
    TTableSchema ToCanonical() const;

    //! Returns (possibly reordered) schema with set key columns.
    TTableSchema ToSorted(const TKeyColumns& keyColumns) const;

    //! Only applies to sorted replicated tables.
    //! Returns the ordered schema used in replication logs.
    TTableSchema ToReplicationLog() const;

    //! Only applies to sorted dynamic tables.
    //! Returns the static schema used for unversioned updates from bulk insert.
    //! Key columns remain unchanged. Additional column |($change_type)| is prepended.
    //! Each value column |name| is replaced with two columns |($value:name)| and |($flags:name)|.
    //! If |sorted| is |false|, sort order is removed from key columns.
    TTableSchema ToUnversionedUpdate(bool sorted = true) const;

    TTableSchema ToModifiedSchema(ETableSchemaModification schemaModification) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    i64 GetMemoryUsage() const;

private:
    int KeyColumnCount_ = 0;
};

void FormatValue(TStringBuilderBase* builder, const TTableSchema& schema, TStringBuf spec);

TString ToString(const TTableSchema& schema);

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema);
void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema);
void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& keyColumnsExt);

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& schema, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TColumnSchema& lhs, const TColumnSchema& rhs);
bool operator != (const TColumnSchema& lhs, const TColumnSchema& rhs);

bool operator == (const TTableSchema& lhs, const TTableSchema& rhs);
bool operator != (const TTableSchema& lhs, const TTableSchema& rhs);

// Compat function for https://st.yandex-team.ru/YT-10668 workaround.
bool IsEqualIgnoringRequiredness(const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns);

void ValidateColumnSchema(
    const TColumnSchema& columnSchema,
    bool isTableSorted = false,
    bool isTableDynamic = false);

void ValidateTableSchema(
    const TTableSchema& schema,
    bool isTableDynamic = false);

void ValidateColumnUniqueness(const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, int> GetLocksMapping(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    std::vector<int>* columnIndexToLockIndex = nullptr,
    std::vector<TString>* lockIndexToName = nullptr);

TLockMask GetLockMask(
    const NTableClient::TTableSchema& schema,
    bool fullAtomicity,
    const std::vector<TString>& locks,
    ELockType lockType = ELockType::SharedWeak);

////////////////////////////////////////////////////////////////////////////////

// NB: Need to place this into NProto for ADL to work properly since TKeyColumns is std::vector.
namespace NProto {

void ToProto(NProto::TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns);
void FromProto(TKeyColumns* keyColumns, const NProto::TKeyColumnsExt& protoKeyColumns);

void ToProto(TColumnFilter* protoColumnFilter, const NTableClient::TColumnFilter& columnFilter);
void FromProto(NTableClient::TColumnFilter* columnFilter, const TColumnFilter& protoColumnFilter);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NTableClient::TColumnSchema>
{
    inline size_t operator()(const NYT::NTableClient::TColumnSchema& columnSchema) const
    {
        return MultiHash(
            columnSchema.Name(),
            *columnSchema.LogicalType(),
            columnSchema.SortOrder(),
            columnSchema.Lock(),
            columnSchema.Expression(),
            columnSchema.Aggregate(),
            columnSchema.Group());
    }
};

template <>
struct THash<NYT::NTableClient::TTableSchema>
{
    inline size_t operator()(const NYT::NTableClient::TTableSchema& tableSchema) const
    {
        size_t result = CombineHashes(THash<bool>()(tableSchema.GetUniqueKeys()), THash<bool>()(tableSchema.GetStrict()));
        if (tableSchema.HasNontrivialSchemaModification()) {
            result = CombineHashes(
                result,
                THash<NYT::NTableClient::ETableSchemaModification>()(tableSchema.GetSchemaModification()));
        }
        for (const auto& columnSchema : tableSchema.Columns()) {
            result = CombineHashes(result, THash<NYT::NTableClient::TColumnSchema>()(columnSchema));
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define SCHEMA_INL_H_
#include "schema-inl.h"
#undef SCHEMA_INL_H_
