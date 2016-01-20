#pragma once

#include "row_base.h"

#include <yt/core/misc/error.h>
#include <yt/core/misc/nullable.h>
#include <yt/core/misc/property.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESortOrder,
    (Ascending)
)

struct TColumnSchema
{
    TColumnSchema();
    TColumnSchema(
        const Stroka& name,
        EValueType type);

    TColumnSchema(const TColumnSchema&) = default;
    TColumnSchema(TColumnSchema&&) = default;

    TColumnSchema& operator=(const TColumnSchema&) = default;
    TColumnSchema& operator=(TColumnSchema&&) = default;

    TColumnSchema& SetSortOrder(const TNullable<ESortOrder>& value);
    TColumnSchema& SetLock(const TNullable<Stroka>& value);
    TColumnSchema& SetExpression(const TNullable<Stroka>& value);
    TColumnSchema& SetAggregate(const TNullable<Stroka>& value);

    Stroka Name;
    EValueType Type;
    TNullable<ESortOrder> SortOrder;
    TNullable<Stroka> Lock;
    TNullable<Stroka> Expression;
    TNullable<Stroka> Aggregate;
};

void Serialize(const TColumnSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TColumnSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema);
void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

////////////////////////////////////////////////////////////////////////////////

class TTableSchema
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TColumnSchema>, Columns);
    DEFINE_BYVAL_RO_PROPERTY(bool, Strict);

    explicit TTableSchema(const std::vector<TColumnSchema>& columns, bool strict = false);
    TTableSchema();
    
    const TColumnSchema* FindColumn(const TStringBuf& name) const;
    const TColumnSchema& GetColumnOrThrow(const TStringBuf& name) const;

    int GetColumnIndex(const TColumnSchema& column) const;
    int GetColumnIndexOrThrow(const TStringBuf& name) const;

    TTableSchema Filter(const TColumnFilter& columnFilter) const;
    TTableSchema TrimNonkeyColumns(const TKeyColumns& keyColumns) const;
    TTableSchema GetPrefix(int length) const;

    // TODO(max42): Get rid of functions below, make schema immutable.

    // These functions perform all necessary validation at each call,
    // so don't call them frequently. If you need to store and modify
    // a set of columns, use std::vector<TColumnSchema> instead.
    void AppendColumn(const TColumnSchema& column);
    void InsertColumn(int position, const TColumnSchema& column);
    void EraseColumn(int position);
    void AlterColumn(int position, const TColumnSchema& column);

    bool HasComputedColumns() const;
    bool IsSorted() const;

    TKeyColumns GetKeyColumns() const;
    int GetKeyColumnCount() const;
    static TTableSchema FromKeyColumns(const TKeyColumns& keyColumns);

    TTableSchema ExtendByNonKeyAnyColumns(const std::vector<Stroka>& columnNames) const;
    TTableSchema ExtendByChannels(const NChunkClient::TChannels& channels) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    void Swap(TTableSchema& other);

private:
    int KeyColumnCount_ = 0;
   
    void ValidateColumnUniqueness();
    void ValidateLocks();
    void ValidateKeyColumnsFormPrefix();
    void ValidateComputedColumns();
    void ValidateAggregatedColumns();
    void Validate();
};

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema);
void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema);
void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& keyColumnsExt);

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TColumnSchema& lhs, const TColumnSchema& rhs);
bool operator != (const TColumnSchema& lhs, const TColumnSchema& rhs);

bool operator == (const TTableSchema& lhs, const TTableSchema& rhs);
bool operator != (const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns);
void ValidateKeyColumnsUpdate(const TKeyColumns& oldKeyColumns, const TKeyColumns& newKeyColumns);

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnSchema(const TColumnSchema& columnSchema);
void ValidateColumnSchemaUpdate(const TColumnSchema& oldColumn, const TColumnSchema& newColumn);

////////////////////////////////////////////////////////////////////////////////

void ValidateDynamicTableConstraints(const TTableSchema& schema);

////////////////////////////////////////////////////////////////////////////////

void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic = false,
    bool isTableEmpty = false);

////////////////////////////////////////////////////////////////////////////////

void ValidatePivotKey(const TOwningKey& pivotKey, const TTableSchema& schema, int keyColumnCount);

////////////////////////////////////////////////////////////////////////////////

// NB: Need to place this into NProto for ADL to work properly since TKeyColumns is std::vector.
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns);
void FromProto(TKeyColumns* keyColumns, const NProto::TKeyColumnsExt& protoKeyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
