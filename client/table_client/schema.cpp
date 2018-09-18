#include "schema.h"
#include "unversioned_row.h"

#include <yt/core/ytree/serialize.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <yt/client/tablet_client/public.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NTabletClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TColumnSchema::TColumnSchema()
    : LogicalType_(ELogicalValueType::Null)
{ }

TColumnSchema::TColumnSchema(
    const TString& name,
    EValueType type,
    TNullable<ESortOrder> SortOrder)
    : Name_(name)
    , LogicalType_(GetLogicalType(type))
    , SortOrder_(SortOrder)
{ }

TColumnSchema::TColumnSchema(
    const TString& name,
    ELogicalValueType type,
    TNullable<ESortOrder> SortOrder)
    : Name_(name)
    , LogicalType_(type)
    , SortOrder_(SortOrder)
{ }

TColumnSchema& TColumnSchema::SetName(const TString& value)
{
    Name_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetSortOrder(const TNullable<ESortOrder>& value)
{
    SortOrder_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetLock(const TNullable<TString>& value)
{
    Lock_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetGroup(const TNullable<TString>& value)
{
    Group_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetExpression(const TNullable<TString>& value)
{
    Expression_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetAggregate(const TNullable<TString>& value)
{
    Aggregate_ = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetLogicalType(ELogicalValueType valueType)
{
    LogicalType_ = valueType;
    return *this;
}

TColumnSchema& TColumnSchema::SetRequired(bool value)
{
    Required_ = value;
    return *this;
}

EValueType TColumnSchema::GetPhysicalType() const
{
    return NTableClient::GetPhysicalType(LogicalType());
}

i64 TColumnSchema::GetMemoryUsage() const
{
    return sizeof(TColumnSchema) +
        Name_.Size() +
        (Lock_ ? Lock_->Size() : 0) +
        (Expression_ ? Expression_->Size() : 0) +
        (Aggregate_ ? Aggregate_->Size() : 0) +
        (Group_ ? Group_->Size() : 0);
}

////////////////////////////////////////////////////////////////////////////////

struct TSerializableColumnSchema
    : public TYsonSerializableLite
    , public TColumnSchema
{
    TSerializableColumnSchema()
    {
        RegisterParameter("name", Name_)
            .NonEmpty();
        RegisterParameter("type", LogicalType_);
        RegisterParameter("lock", Lock_)
            .Default();
        RegisterParameter("expression", Expression_)
            .Default();
        RegisterParameter("aggregate", Aggregate_)
            .Default();
        RegisterParameter("sort_order", SortOrder_)
            .Default();
        RegisterParameter("group", Group_)
            .Default();
        RegisterParameter("required", Required_)
            .Default(false);

        RegisterPostprocessor([&] () {
            // Name
            if (Name().empty()) {
                THROW_ERROR_EXCEPTION("Column name cannot be empty");
            }

            try {
                // Required
                if (LogicalType() == ELogicalValueType::Any && Required()) {
                    THROW_ERROR_EXCEPTION("Column of type %Qlv cannot be \"required\"",
                        ELogicalValueType::Any);
                }

                // Lock
                if (Lock() && Lock()->empty()) {
                    THROW_ERROR_EXCEPTION("Lock name cannot be empty");
                }

                // Group
                if (Group() && Group()->empty()) {
                    THROW_ERROR_EXCEPTION("Group name cannot be empty");
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error validating column %Qv in table schema",
                    Name())
                    << ex;
            }
        });
    }
};

void Serialize(const TColumnSchema& schema, IYsonConsumer* consumer)
{
    TSerializableColumnSchema wrapper;
    static_cast<TColumnSchema&>(wrapper) = schema;
    Serialize(static_cast<const TYsonSerializableLite&>(wrapper), consumer);
}

void Deserialize(TColumnSchema& schema, INodePtr node)
{
    TSerializableColumnSchema wrapper;
    Deserialize(static_cast<TYsonSerializableLite&>(wrapper), node);
    schema = static_cast<TColumnSchema&>(wrapper);
}

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema)
{
    protoSchema->set_name(schema.Name());
    protoSchema->set_type(static_cast<int>(schema.GetPhysicalType()));
    protoSchema->set_logical_type(static_cast<int>(schema.LogicalType()));
    if (schema.Lock()) {
        protoSchema->set_lock(*schema.Lock());
    }
    if (schema.Expression()) {
        protoSchema->set_expression(*schema.Expression());
    }
    if (schema.Aggregate()) {
        protoSchema->set_aggregate(*schema.Aggregate());
    }
    if (schema.SortOrder()) {
        protoSchema->set_sort_order(static_cast<int>(*schema.SortOrder()));
    }
    if (schema.Group()) {
        protoSchema->set_group(*schema.Group());
    }
    if (schema.Required()) {
        protoSchema->set_required(schema.Required());
    }
}

void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema)
{
    schema->SetName(protoSchema.name());
    if (protoSchema.has_logical_type()) {
        schema->SetLogicalType(static_cast<ELogicalValueType>(protoSchema.logical_type()));
        YCHECK(schema->GetPhysicalType() == static_cast<EValueType>(protoSchema.type()));
    } else {
        schema->SetLogicalType(GetLogicalType(static_cast<EValueType>(protoSchema.type())));
    }
    schema->SetLock(protoSchema.has_lock() ? MakeNullable(protoSchema.lock()) : Null);
    schema->SetExpression(protoSchema.has_expression() ? MakeNullable(protoSchema.expression()) : Null);
    schema->SetAggregate(protoSchema.has_aggregate() ? MakeNullable(protoSchema.aggregate()) : Null);
    schema->SetSortOrder(protoSchema.has_sort_order() ? MakeNullable(ESortOrder(protoSchema.sort_order())) : Null);
    schema->SetGroup(protoSchema.has_group() ? MakeNullable(protoSchema.group()) : Null);
    schema->SetRequired(protoSchema.required());
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema::TTableSchema()
    : Strict_(false)
    , UniqueKeys_(false)
{ }

TTableSchema::TTableSchema(
    std::vector<TColumnSchema> columns,
    bool strict,
    bool uniqueKeys)
    : Columns_(std::move(columns))
    , Strict_(strict)
    , UniqueKeys_(uniqueKeys)
{
    for (const auto& column : Columns_) {
        if (column.SortOrder()) {
            ++KeyColumnCount_;
        }
    }
}

const TColumnSchema* TTableSchema::FindColumn(TStringBuf name) const
{
    for (auto& column : Columns_) {
        if (column.Name() == name) {
            return &column;
        }
    }
    return nullptr;
}

const TColumnSchema& TTableSchema::GetColumn(TStringBuf name) const
{
    auto* column = FindColumn(name);
    YCHECK(column);
    return *column;
}

const TColumnSchema& TTableSchema::GetColumnOrThrow(TStringBuf name) const
{
    auto* column = FindColumn(name);
    if (!column) {
        THROW_ERROR_EXCEPTION("Missing schema column %Qv", name);
    }
    return *column;
}

int TTableSchema::GetColumnIndex(const TColumnSchema& column) const
{
    return &column - Columns().data();
}

int TTableSchema::GetColumnIndex(TStringBuf name) const
{
    return GetColumnIndex(GetColumn(name));
}

int TTableSchema::GetColumnIndexOrThrow(TStringBuf name) const
{
    return GetColumnIndex(GetColumnOrThrow(name));
}

TTableSchema TTableSchema::Filter(const TColumnFilter& columnFilter) const
{
    if (columnFilter.IsUniversal()) {
        return *this;
    }

    int newKeyColumnCount = 0;
    bool inKeyColumns = true;
    std::vector<TColumnSchema> columns;
    for (int id : columnFilter.GetIndexes()) {
        if (id < 0 || id >= Columns_.size()) {
            THROW_ERROR_EXCEPTION("Invalid column id in filter: excepted in range [0, %v], got %v",
                Columns_.size() - 1,
                id);
        }

        if (id != columns.size() || !Columns_[id].SortOrder()) {
            inKeyColumns = false;
        }

        columns.push_back(Columns_[id]);

        if (!inKeyColumns) {
            columns.back().SetSortOrder(Null);
        }

        if (columns.back().SortOrder()) {
            ++newKeyColumnCount;
        }
    }

    return TTableSchema(
        std::move(columns),
        Strict_,
        UniqueKeys_ && (newKeyColumnCount == GetKeyColumnCount()));
}

TTableSchema TTableSchema::Filter(const THashSet<TString>& columns) const
{
    TColumnFilter::TIndexes indexes;
    for (const auto& column : Columns()) {
        if (columns.find(column.Name()) != columns.end()) {
            indexes.push_back(GetColumnIndex(column));
        }
    }
    return Filter(TColumnFilter(std::move(indexes)));
}

TTableSchema TTableSchema::Filter(const TNullable<std::vector<TString>>& columns) const
{
    if (!columns) {
        return *this;
    }

    return Filter(THashSet<TString>(columns->begin(), columns->end()));
}

bool TTableSchema::HasComputedColumns() const
{
    for (const auto& column : Columns()) {
        if (column.Expression()) {
            return true;
        }
    }
    return false;
}

bool TTableSchema::IsSorted() const
{
    return KeyColumnCount_ > 0;
}

bool TTableSchema::IsUniqueKeys() const
{
    return UniqueKeys_;
}

TKeyColumns TTableSchema::GetKeyColumns() const
{
    TKeyColumns keyColumns;
    for (const auto& column : Columns()) {
        if (column.SortOrder()) {
            keyColumns.push_back(column.Name());
        }
    }
    return keyColumns;
}

int TTableSchema::GetColumnCount() const
{
    return static_cast<int>(Columns_.size());
}

int TTableSchema::GetKeyColumnCount() const
{
    return KeyColumnCount_;
}

int TTableSchema::GetValueColumnCount() const
{
    return GetColumnCount() - GetKeyColumnCount();
}

TTableSchema TTableSchema::FromKeyColumns(const TKeyColumns& keyColumns)
{
    TTableSchema schema;
    for (const auto& columnName : keyColumns) {
        schema.Columns_.push_back(
            TColumnSchema(columnName, ELogicalValueType::Any)
                .SetSortOrder(ESortOrder::Ascending));
    }
    schema.KeyColumnCount_ = keyColumns.size();
    ValidateTableSchema(schema);
    return schema;
}

TTableSchema TTableSchema::ToQuery() const
{
    if (IsSorted()) {
        return *this;
    } else {
        std::vector<TColumnSchema> columns {
            TColumnSchema(TabletIndexColumnName, ELogicalValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema(RowIndexColumnName, ELogicalValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
        };
        columns.insert(columns.end(), Columns_.begin(), Columns_.end());
        return TTableSchema(std::move(columns));
    }
}

TTableSchema TTableSchema::ToWrite() const
{
    std::vector<TColumnSchema> columns;
    if (IsSorted()) {
        for (const auto& column : Columns_) {
            if (!column.Expression()) {
                columns.push_back(column);
            }
        }
    } else {
        columns.push_back(TColumnSchema(TabletIndexColumnName, ELogicalValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending));
        for (const auto& column : Columns_) {
            if (column.Name() != TimestampColumnName) {
                columns.push_back(column);
            }
        }
    }
    return TTableSchema(std::move(columns), Strict_, UniqueKeys_);
}

TTableSchema TTableSchema::ToVersionedWrite() const
{
    return *this;
}

TTableSchema TTableSchema::ToLookup() const
{
    std::vector<TColumnSchema> columns;
    for (const auto& column : Columns_) {
        if (column.SortOrder() && !column.Expression()) {
            columns.push_back(column);
        }
    }
    return TTableSchema(std::move(columns), Strict_, UniqueKeys_);
}

TTableSchema TTableSchema::ToDelete() const
{
    return ToLookup();
}

TTableSchema TTableSchema::ToKeys() const
{
    std::vector<TColumnSchema> columns(Columns_.begin(), Columns_.begin() + KeyColumnCount_);
    return TTableSchema(std::move(columns), Strict_, UniqueKeys_);
}

TTableSchema TTableSchema::ToValues() const
{
    std::vector<TColumnSchema> columns(Columns_.begin() + KeyColumnCount_, Columns_.end());
    return TTableSchema(std::move(columns), Strict_, false);
}

TTableSchema TTableSchema::ToUniqueKeys() const
{
    return TTableSchema(Columns_, Strict_, true);
}

TTableSchema TTableSchema::ToStrippedColumnAttributes() const
{
    std::vector<TColumnSchema> strippedColumns;
    for (auto& column : Columns_) {
        strippedColumns.emplace_back(column.Name(), column.LogicalType());
        strippedColumns.back().SetRequired(column.Required());
    }
    return TTableSchema(strippedColumns, Strict_, false);
}

TTableSchema TTableSchema::ToSortedStrippedColumnAttributes() const
{
    std::vector<TColumnSchema> strippedColumns;
    for (auto& column : Columns_) {
        strippedColumns.emplace_back(column.Name(), column.LogicalType(), column.SortOrder());
        strippedColumns.back().SetRequired(column.Required());
    }
    return TTableSchema(strippedColumns, Strict_, UniqueKeys_);
}

TTableSchema TTableSchema::ToCanonical() const
{
    auto columns = Columns();
    std::sort(
        columns.begin() + KeyColumnCount_,
        columns.end(),
        [] (const TColumnSchema& lhs, const TColumnSchema& rhs) {
            return lhs.Name() < rhs.Name();
        });
    return TTableSchema(columns, Strict_, UniqueKeys_);
}

TTableSchema TTableSchema::ToSorted(const TKeyColumns& keyColumns) const
{
    int oldKeyColumnCount = 0;
    auto columns = Columns();
    for (int index = 0; index < keyColumns.size(); ++index) {
        auto it = std::find_if(
            columns.begin() + index,
            columns.end(),
            [&] (const TColumnSchema& column) {
                return column.Name() == keyColumns[index];
            });

        if (it == columns.end()) {
            THROW_ERROR_EXCEPTION("Column %Qv is not found in schema", keyColumns[index])
                << TErrorAttribute("schema", *this)
                << TErrorAttribute("key_columns", keyColumns);
        }

        if (it->SortOrder()) {
            ++oldKeyColumnCount;
        }

        std::swap(columns[index], *it);
        columns[index].SetSortOrder(ESortOrder::Ascending);
    }

    auto uniqueKeys = UniqueKeys_ && oldKeyColumnCount == GetKeyColumnCount();

    for (auto it = columns.begin() + keyColumns.size(); it != columns.end(); ++it) {
        it->SetSortOrder(Null);
    }

    return TTableSchema(columns, Strict_, uniqueKeys);
}

TTableSchema TTableSchema::ToReplicationLog() const
{
    std::vector<TColumnSchema> columns;
    columns.push_back(TColumnSchema(TimestampColumnName, ELogicalValueType::Uint64));
    if (IsSorted()) {
        columns.push_back(TColumnSchema(TReplicationLogTable::ChangeTypeColumnName, ELogicalValueType::Int64));
        for (const auto& column : Columns_) {
            if (column.SortOrder()) {
                columns.push_back(
                    TColumnSchema(TReplicationLogTable::KeyColumnNamePrefix + column.Name(), column.LogicalType()));
            } else {
                columns.push_back(
                    TColumnSchema(TReplicationLogTable::ValueColumnNamePrefix + column.Name(), column.LogicalType()));
                columns.push_back(
                    TColumnSchema(TReplicationLogTable::FlagsColumnNamePrefix + column.Name(), ELogicalValueType::Uint64));
            }
        }
    } else {
        for (const auto& column : Columns_) {
            columns.push_back(
                TColumnSchema(TReplicationLogTable::ValueColumnNamePrefix + column.Name(), column.LogicalType()));
        }
    }
    return TTableSchema(std::move(columns), true, false);
}

void TTableSchema::Save(TStreamSaveContext& context) const
{
    using NYT::Save;
    Save(context, ToProto<NTableClient::NProto::TTableSchemaExt>(*this));
}

void TTableSchema::Load(TStreamLoadContext& context)
{
    using NYT::Load;
    auto protoSchema = NYT::Load<NTableClient::NProto::TTableSchemaExt>(context);
    *this = FromProto<TTableSchema>(protoSchema);
}

i64 TTableSchema::GetMemoryUsage() const
{
    i64 usage = sizeof(TTableSchema);
    for (const auto& column : Columns_) {
        usage += column.GetMemoryUsage();
    }
    return usage;
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TTableSchema& schema)
{
    return ConvertToYsonString(schema, EYsonFormat::Text).GetData();
}

void Serialize(const TTableSchema& schema, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("strict").Value(schema.GetStrict())
            .Item("unique_keys").Value(schema.GetUniqueKeys())
        .EndAttributes()
        .Value(schema.Columns());
}

void Deserialize(TTableSchema& schema, INodePtr node)
{
    schema = TTableSchema(
        ConvertTo<std::vector<TColumnSchema>>(node),
        node->Attributes().Get<bool>("strict", true),
        node->Attributes().Get<bool>("unique_keys", false));
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema)
{
    ToProto(protoSchema->mutable_columns(), schema.Columns());
    protoSchema->set_strict(schema.GetStrict());
    protoSchema->set_unique_keys(schema.GetUniqueKeys());
}

void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema)
{
    *schema = TTableSchema(
        FromProto<std::vector<TColumnSchema>>(protoSchema.columns()),
        protoSchema.strict(),
        protoSchema.unique_keys());
}

void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& protoKeyColumns)
{
    auto columns = FromProto<std::vector<TColumnSchema>>(protoSchema.columns());
    for (int columnIndex = 0; columnIndex < protoKeyColumns.names_size(); ++columnIndex) {
        auto& columnSchema = columns[columnIndex];
        YCHECK(columnSchema.Name() == protoKeyColumns.names(columnIndex));
        columnSchema.SetSortOrder(ESortOrder::Ascending);
    }
    for (int columnIndex = protoKeyColumns.names_size(); columnIndex < columns.size(); ++columnIndex) {
        auto& columnSchema = columns[columnIndex];
        YCHECK(!columnSchema.SortOrder());
    }
    *schema = TTableSchema(
        std::move(columns),
        protoSchema.strict(),
        protoSchema.unique_keys());
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return lhs.Name() == rhs.Name()
           && lhs.LogicalType() == rhs.LogicalType()
           && lhs.Required() == rhs.Required()
           && lhs.SortOrder() == rhs.SortOrder()
           && lhs.Aggregate() == rhs.Aggregate()
           && lhs.Expression() == rhs.Expression();
}

bool operator!=(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return !(lhs == rhs);
}

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return lhs.Columns() == rhs.Columns() &&
        lhs.GetStrict() == rhs.GetStrict() &&
        lhs.GetUniqueKeys() == rhs.GetUniqueKeys();
}

bool operator!=(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

bool IsSubtypeOf(ELogicalValueType lhs, ELogicalValueType rhs)
{
    if (lhs == rhs) {
        return true;
    }
    if (rhs == ELogicalValueType::Any) {
        return true;
    }

    auto leftPhysicalType = GetPhysicalType(lhs);
    auto rightPhysicalType = GetPhysicalType(rhs);
    if (leftPhysicalType != rightPhysicalType) {
        return false;
    }

    if (leftPhysicalType == EValueType::Uint64 || leftPhysicalType == EValueType::Int64) {
        static const std::vector<ELogicalValueType> order = {
            ELogicalValueType::Uint8,
            ELogicalValueType::Int8,
            ELogicalValueType::Uint16,
            ELogicalValueType::Int16,
            ELogicalValueType::Uint32,
            ELogicalValueType::Int32,
            ELogicalValueType::Uint64,
            ELogicalValueType::Int64,
        };

        auto lit = std::find(order.begin(), order.end(), lhs);
        auto rit = std::find(order.begin(), order.end(), rhs);
        Y_ASSERT(lit != order.end());
        Y_ASSERT(rit != order.end());

        return lit <= rit;
    }

    if (leftPhysicalType == EValueType::String) {
        static const std::vector<ELogicalValueType> order = {
            ELogicalValueType::Utf8,
            ELogicalValueType::String,
        };
        auto lit = std::find(order.begin(), order.end(), lhs);
        auto rit = std::find(order.begin(), order.end(), rhs);
        Y_ASSERT(lit != order.end());
        Y_ASSERT(rit != order.end());
        return lit <= rit;
    }

    return false;
}

void ValidateKeyColumns(const TKeyColumns& keyColumns)
{
    ValidateKeyColumnCount(keyColumns.size());

    THashSet<TString> names;
    for (const auto& name : keyColumns) {
        if (!names.insert(name).second) {
            THROW_ERROR_EXCEPTION("Duplicate key column name %Qv",
                name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnSchema(
    const TColumnSchema& columnSchema,
    bool isTableSorted,
    bool isTableDynamic)
{
    static const auto allowedAggregates = THashSet<TString>{
        "sum",
        "min",
        "max",
        "first"
    };

    static const auto allowedSortedTablesSystemColumns = THashMap<TString, EValueType>{
    };

    static const auto allowedOrderedTablesSystemColumns = THashMap<TString, EValueType>{
        {TimestampColumnName, EValueType::Uint64}
    };

    const auto& name = columnSchema.Name();
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Column name cannot be empty");
    }

    try {
        if (name.StartsWith(SystemColumnNamePrefix)) {
            const auto& allowedSystemColumns = isTableSorted
                ? allowedSortedTablesSystemColumns
                : allowedOrderedTablesSystemColumns;
            auto it = allowedSystemColumns.find(name);
            if (it == allowedSystemColumns.end()) {
                THROW_ERROR_EXCEPTION("System column name %Qv is not allowed here",
                    name);
            }
            if (columnSchema.GetPhysicalType() != it->second) {
                THROW_ERROR_EXCEPTION("Invalid type of column name %Qv: expected %Qlv, got %Qlv",
                    name,
                    it->second,
                    columnSchema.GetPhysicalType());
            }
        }

        if (name.size() > MaxColumnNameLength) {
            THROW_ERROR_EXCEPTION("Column name is longer than maximum allowed: %v > %v",
                columnSchema.Name().size(),
                MaxColumnNameLength);
        }

        if (columnSchema.LogicalType() == ELogicalValueType::Any && columnSchema.Required()) {
            THROW_ERROR_EXCEPTION("Column of type %Qlv cannot be required",
                ELogicalValueType::Any);
        }

        if (columnSchema.Lock()) {
            if (columnSchema.Lock()->empty()) {
                THROW_ERROR_EXCEPTION("Column lock name cannot be empty");
            }
            if (columnSchema.Lock()->size() > MaxColumnLockLength) {
                THROW_ERROR_EXCEPTION("Column lock name is longer than maximum allowed: %v > %v",
                    columnSchema.Lock()->size(),
                    MaxColumnLockLength);
            }
            if (columnSchema.SortOrder()) {
                THROW_ERROR_EXCEPTION("Column lock cannot be set on a key column");
            }
        }

        if (columnSchema.Group()) {
            if (columnSchema.Group()->empty()) {
                THROW_ERROR_EXCEPTION("Column group should either be unset or be non-empty");
            }
            if (columnSchema.Group()->size() > MaxColumnGroupLength) {
                THROW_ERROR_EXCEPTION("Column group name is longer than maximum allowed: %v > %v",
                    columnSchema.Group()->size(),
                    MaxColumnGroupLength);
            }
        }

        ValidateSchemaValueType(columnSchema.GetPhysicalType());

        if (columnSchema.Expression() && !columnSchema.SortOrder() && isTableDynamic) {
            THROW_ERROR_EXCEPTION("Non-key column cannot be computed");
        }

        if (columnSchema.Aggregate() && columnSchema.SortOrder()) {
            THROW_ERROR_EXCEPTION("Key column cannot be aggregated");
        }

        if (columnSchema.Aggregate() && allowedAggregates.find(*columnSchema.Aggregate()) == allowedAggregates.end()) {
            THROW_ERROR_EXCEPTION("Invalid aggregate function %Qv",
                *columnSchema.Aggregate());
        }

        if (columnSchema.Expression() && columnSchema.Required()) {
            THROW_ERROR_EXCEPTION("Computed column cannot be required");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating schema of a column %Qv",
            name)
            << ex;
    }
}

void ValidateDynamicTableConstraints(const TTableSchema& schema)
{
    if (!schema.GetStrict()) {
        THROW_ERROR_EXCEPTION("\"strict\" cannot be \"false\" for a dynamic table");
    }

    if (schema.IsSorted() && !schema.GetUniqueKeys()) {
        THROW_ERROR_EXCEPTION("\"unique_keys\" cannot be \"false\" for a sorted dynamic table");
    }

    if (schema.GetKeyColumnCount() == schema.Columns().size()) {
       THROW_ERROR_EXCEPTION("There must be at least one non-key column");
    }

    for (const auto& column : schema.Columns()) {
        try {
            if (column.SortOrder() && column.GetPhysicalType() == EValueType::Any) {
                THROW_ERROR_EXCEPTION("Dynamic table cannot have key column of type: %Qv",
                    column.GetPhysicalType());
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error validating column %Qv in dynamic table schema",
                column.Name())
                << ex;
        }
    }
}

//! Validates that there are no duplicates among the column names.
void ValidateColumnUniqueness(const TTableSchema& schema)
{
    THashSet<TString> columnNames;
    for (const auto& column : schema.Columns()) {
        if (!columnNames.insert(column.Name()).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in table schema",
                column.Name());
        }
    }
}

//! Validates that number of locks doesn't exceed #MaxColumnLockCount.
void ValidateLocks(const TTableSchema& schema)
{
    THashSet<TString> lockNames;
    YCHECK(lockNames.insert(PrimaryLockName).second);
    for (const auto& column : schema.Columns()) {
        if (column.Lock()) {
            lockNames.insert(*column.Lock());
        }
    }

    if (lockNames.size() > MaxColumnLockCount) {
        THROW_ERROR_EXCEPTION("Too many column locks in table schema: actual %v, limit %v",
            lockNames.size(),
            MaxColumnLockCount);
    }
}

//! Validates that key columns form a prefix of a table schema.
void ValidateKeyColumnsFormPrefix(const TTableSchema& schema)
{
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        if (!schema.Columns()[index].SortOrder()) {
            THROW_ERROR_EXCEPTION("Key columns must form a prefix of schema");
        }
    }
    // The fact that first GetKeyColumnCount() columns have SortOrder automatically
    // implies that the rest of columns don't have SortOrder, so we don't need to check it.
}

//! Validates |$timestamp| column, if any.
/*!
 *  Validate that:
 *  - |$timestamp| column cannot be a part of key.
 *  - |$timestamp| column can only be present in unsorted tables.
 *  - |$timestamp| column has type |uint64|.
 */
void ValidateTimestampColumn(const TTableSchema& schema)
{
    auto* column = schema.FindColumn(TimestampColumnName);
    if (!column) {
        return;
    }

    if (column->SortOrder()) {
        THROW_ERROR_EXCEPTION("%Qv column cannot be a part of key",
            TimestampColumnName);
    }

    if (column->LogicalType() != ELogicalValueType::Uint64) {
        THROW_ERROR_EXCEPTION("%Qv column must have %Qlv type",
            TimestampColumnName,
            EValueType::Uint64);
    }

    if (schema.IsSorted()) {
        THROW_ERROR_EXCEPTION("%Qv column cannot appear in a sorted table",
            TimestampColumnName);
    }
}

// Validate schema attributes.
void ValidateSchemaAttributes(const TTableSchema& schema)
{
    if (schema.GetUniqueKeys() && schema.GetKeyColumnCount() == 0) {
        THROW_ERROR_EXCEPTION("\"unique_keys\" can only be true if key columns are present");
    }
}

void ValidateTableSchema(const TTableSchema& schema, bool isTableDynamic)
{
    for (const auto& column : schema.Columns()) {
        ValidateColumnSchema(
            column,
            schema.IsSorted(),
            isTableDynamic);
    }
    ValidateColumnUniqueness(schema);
    ValidateLocks(schema);
    ValidateKeyColumnsFormPrefix(schema);
    ValidateTimestampColumn(schema);
    ValidateSchemaAttributes(schema);
    if (isTableDynamic) {
        ValidateDynamicTableConstraints(schema);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

using NYT::ToProto;
using NYT::FromProto;

void ToProto(TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns)
{
    ToProto(protoKeyColumns->mutable_names(), keyColumns);
}

void FromProto(TKeyColumns* keyColumns, const TKeyColumnsExt& protoKeyColumns)
{
    *keyColumns = FromProto<TKeyColumns>(protoKeyColumns.names());
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
