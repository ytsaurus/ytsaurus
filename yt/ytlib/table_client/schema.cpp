#include "schema.h"
#include "unversioned_row.h"

#include <yt/core/ytree/serialize.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/table_client/chunk_meta.pb.h>

#include <yt/ytlib/chunk_client/schema.h>

// TODO(sandello,lukyan): Refine these dependencies.
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NYson;
using namespace NQueryClient;
using namespace NChunkClient;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TColumnSchema::TColumnSchema()
    : Type(EValueType::Null)
{ }

TColumnSchema::TColumnSchema(
    const Stroka& name,
    EValueType type)
    : Name(name)
    , Type(type)
{ }

TColumnSchema& TColumnSchema::SetSortOrder(const TNullable<ESortOrder>& value)
{
    SortOrder = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetLock(const TNullable<Stroka>& value)
{
    Lock = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetGroup(const TNullable<Stroka>& value)
{
    Group = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetExpression(const TNullable<Stroka>& value)
{
    Expression = value;
    return *this;
}

TColumnSchema& TColumnSchema::SetAggregate(const TNullable<Stroka>& value)
{
    Aggregate = value;
    return *this;
}

struct TSerializableColumnSchema
    : public TYsonSerializableLite
        , public TColumnSchema
{
    TSerializableColumnSchema()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("type", Type);
        RegisterParameter("lock", Lock)
            .Default();
        RegisterParameter("expression", Expression)
            .Default();
        RegisterParameter("aggregate", Aggregate)
            .Default();
        RegisterParameter("sort_order", SortOrder)
            .Default();

        RegisterValidator(
            [ & ]() {
                // Name
                if (Name.empty()) {
                    THROW_ERROR_EXCEPTION("Column name cannot be empty");
                }

                // Type
                try {
                    ValidateSchemaValueType(Type);
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error validating column %Qv in table schema",
                        Name)
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
    protoSchema->set_name(schema.Name);
    protoSchema->set_type(static_cast<int>(schema.Type));
    if (schema.Lock) {
        protoSchema->set_lock(*schema.Lock);
    }
    if (schema.Expression) {
        protoSchema->set_expression(*schema.Expression);
    }
    if (schema.Aggregate) {
        protoSchema->set_aggregate(*schema.Aggregate);
    }
    if (schema.SortOrder) {
        protoSchema->set_sort_order(static_cast<int>(*schema.SortOrder));
    }
}

void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema)
{
    schema->Name = protoSchema.name();
    schema->Type = EValueType(protoSchema.type());
    schema->Lock = protoSchema.has_lock() ? MakeNullable(protoSchema.lock()) : Null;
    schema->Expression = protoSchema.has_expression() ? MakeNullable(protoSchema.expression()) : Null;
    schema->Aggregate = protoSchema.has_aggregate() ? MakeNullable(protoSchema.aggregate()) : Null;
    schema->SortOrder = protoSchema.has_sort_order() ? MakeNullable(ESortOrder(protoSchema.sort_order())) : Null;
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema::TTableSchema()
    : Strict_(false)
{ }

TTableSchema::TTableSchema(
    const std::vector<TColumnSchema>& columns,
    bool strict)
    : Columns_(columns)
      , Strict_(strict)
{
    for (const auto& column : Columns_) {
        if (column.SortOrder) {
            ++KeyColumnCount_;
        }
    }
}

const TColumnSchema* TTableSchema::FindColumn(const TStringBuf& name) const
{
    for (auto& column : Columns_) {
        if (column.Name == name) {
            return &column;
        }
    }
    return nullptr;
}

const TColumnSchema& TTableSchema::GetColumnOrThrow(const TStringBuf& name) const
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

int TTableSchema::GetColumnIndexOrThrow(const TStringBuf& name) const
{
    return GetColumnIndex(GetColumnOrThrow(name));
}

TTableSchema TTableSchema::Filter(const TColumnFilter& columnFilter) const
{
    if (columnFilter.All) {
        return *this;
    }

    TTableSchema result;
    for (int id : columnFilter.Indexes) {
        if (id < 0 || id >= Columns_.size()) {
            THROW_ERROR_EXCEPTION("Invalid column id in filter: excepted in range [0, %v], got %v",
                Columns_.size() - 1,
                id);
        }
        result.Columns_.push_back(Columns_[id]);
    }
    return result;
}

void TTableSchema::AppendColumn(const TColumnSchema& column)
{
    TTableSchema temp = *this;
    temp.Columns_.push_back(column);
    if (column.SortOrder) {
        ++temp.KeyColumnCount_;
    }
    // XXX(babenko): this line below was commented out since we must allow
    // system columns in query schema
    // ValidateTableSchema(temp);
    this->Swap(temp);
}

bool TTableSchema::HasComputedColumns() const
{
    for (const auto& column : Columns()) {
        if (column.Expression) {
            return true;
        }
    }
    return false;
}

bool TTableSchema::IsSorted() const
{
    return KeyColumnCount_ > 0;
}

TKeyColumns TTableSchema::GetKeyColumns() const
{
    TKeyColumns keyColumns;
    for (const auto& column : Columns()) {
        if (column.SortOrder) {
            keyColumns.push_back(column.Name);
        }
    }
    return keyColumns;
}

int TTableSchema::GetKeyColumnCount() const
{
    return KeyColumnCount_;
}

TTableSchema TTableSchema::FromKeyColumns(const TKeyColumns& keyColumns)
{
    TTableSchema schema;
    for (const auto& columnName : keyColumns) {
        schema.Columns_.push_back(
            TColumnSchema(columnName, EValueType::Any)
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
            TColumnSchema(TabletIndexColumnName, EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema(RowIndexColumnName, EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
        };
        columns.insert(columns.end(), Columns_.begin(), Columns_.end());
        return TTableSchema(columns);
    }
}

TTableSchema TTableSchema::ToWrite() const
{
    if (IsSorted()) {
        return *this;
    } else {
        std::vector<TColumnSchema> columns {
            TColumnSchema(TabletIndexColumnName, EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
        };
        columns.insert(columns.end(), Columns_.begin(), Columns_.end());
        return TTableSchema(columns);
    }
}

TTableSchema TTableSchema::ToKeys()
{
    std::vector<TColumnSchema> columns(Columns_.begin(), Columns_.begin() + KeyColumnCount_);
    return TTableSchema(columns);
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

void TTableSchema::Swap(TTableSchema& other)
{
    Columns_.swap(other.Columns_);
    std::swap(Strict_, other.Strict_);
    std::swap(KeyColumnCount_, other.KeyColumnCount_);
}

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(const TTableSchema& schema)
{
    return ConvertToYsonString(schema, EYsonFormat::Text).Data();
}

void Serialize(const TTableSchema& schema, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("strict").Value(schema.GetStrict())
        .EndAttributes()
        .Value(schema.Columns());
}

void Deserialize(TTableSchema& schema, INodePtr node)
{
    schema = TTableSchema(
        ConvertTo<std::vector<TColumnSchema>>(node),
        node->Attributes().Get<bool>("strict", true));
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema)
{
    ToProto(protoSchema->mutable_columns(), schema.Columns());
    protoSchema->set_strict(schema.GetStrict());
}

void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema)
{
    *schema = TTableSchema(
        FromProto<std::vector<TColumnSchema>>(protoSchema.columns()),
        protoSchema.strict());
}

void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& protoKeyColumns)
{
    auto columns = FromProto<std::vector<TColumnSchema>>(protoSchema.columns());
    for (int columnIndex = 0; columnIndex < protoKeyColumns.names_size(); ++columnIndex) {
        auto& columnSchema = columns[columnIndex];
        YCHECK(columnSchema.Name == protoKeyColumns.names(columnIndex));
        YCHECK(!columnSchema.SortOrder);
        columnSchema.SortOrder = ESortOrder::Ascending;
    }
    *schema = TTableSchema(
        columns,
        protoSchema.strict());
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return lhs.Name == rhs.Name
           && lhs.Type == rhs.Type
           && lhs.SortOrder == rhs.SortOrder
           && lhs.Aggregate == rhs.Aggregate
           && lhs.Expression == rhs.Expression;
}

bool operator!=(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return !(lhs == rhs);
}

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return lhs.Columns() == rhs.Columns() && lhs.GetStrict() == rhs.GetStrict();
}

bool operator!=(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns)
{
    ValidateKeyColumnCount(keyColumns.size());

    yhash_set<Stroka> names;
    for (const auto& name : keyColumns) {
        if (!names.insert(name).second) {
            THROW_ERROR_EXCEPTION("Duplicate key column name %Qv",
                name);
        }
    }
}

void ValidateKeyColumnsUpdate(const TKeyColumns& oldKeyColumns, const TKeyColumns& newKeyColumns)
{
    ValidateKeyColumns(newKeyColumns);

    for (int index = 0; index < std::max(oldKeyColumns.size(), newKeyColumns.size()); ++index) {
        if (index >= newKeyColumns.size()) {
            THROW_ERROR_EXCEPTION("Missing original key column %Qv",
                oldKeyColumns[index]);
        } else if (index >= oldKeyColumns.size()) {
            // This is fine; new key column is added
        } else {
            if (oldKeyColumns[index] != newKeyColumns[index]) {
                THROW_ERROR_EXCEPTION("Key column mismatch in position %v: expected %Qv, got %Qv",
                    index,
                    oldKeyColumns[index],
                    newKeyColumns[index]);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnSchema(const TColumnSchema& columnSchema)
{
    try {
        if (columnSchema.Name.empty()) {
            THROW_ERROR_EXCEPTION("Column name cannot be empty");
        }

        if (columnSchema.Name.has_prefix(SystemColumnNamePrefix)) {
            THROW_ERROR_EXCEPTION("Column name cannot start with prefix %Qv",
                SystemColumnNamePrefix);
        }

        if (columnSchema.Name.size() > MaxColumnNameLength) {
            THROW_ERROR_EXCEPTION("Column name is longer than maximum allowed: %v > %v",
                columnSchema.Name.size(),
                MaxColumnNameLength);
        }

        if (columnSchema.Lock) {
            if (columnSchema.Lock->empty()) {
                THROW_ERROR_EXCEPTION("Column lock should either be unset or be non-empty");
            }
            if (columnSchema.Lock->size() > MaxColumnLockLength) {
                THROW_ERROR_EXCEPTION("Column lock name is longer than maximum allowed: %v > %v",
                    columnSchema.Lock->size(),
                    MaxColumnLockLength);
            }
            if (columnSchema.SortOrder) {
                THROW_ERROR_EXCEPTION("Column lock cannot be set on a key column");
            }
        }

        if (columnSchema.Group) {
            if (columnSchema.Group->empty()) {
                THROW_ERROR_EXCEPTION("Column group should either be unset or be non-empty");
            }
            if (columnSchema.Group->size() > MaxColumnGroupLength) {
                THROW_ERROR_EXCEPTION("Column group name is longer than maximum allowed: %v > %v",
                    columnSchema.Group->size(),
                    MaxColumnGroupLength);
            }
        }

        ValidateSchemaValueType(columnSchema.Type);

        if (columnSchema.Expression && !columnSchema.SortOrder) {
            THROW_ERROR_EXCEPTION("Non-key column cannot be computed");
        }

        if (columnSchema.Aggregate && columnSchema.SortOrder) {
            THROW_ERROR_EXCEPTION("Key column cannot be aggregated");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating schema of a column %Qv",
            columnSchema.Name)
                << ex;
    }
}

//! Validates the column schema update.
/*!
 *  \pre{oldColumn and newColumn should have the same name.}
 *
 *  Validates that:
 *  - Column type remains the same.
 *  - Column expression remains the same.
 *  - Column aggregate method either was introduced or remains the same.
 */
void ValidateColumnSchemaUpdate(const TColumnSchema& oldColumn, const TColumnSchema& newColumn)
{
    YCHECK(oldColumn.Name == newColumn.Name);
    if (newColumn.Type != oldColumn.Type) {
        THROW_ERROR_EXCEPTION("Type mismatch for column %Qv: old %Qlv, new %Qlv",
            oldColumn.Name,
            oldColumn.Type,
            newColumn.Type);
    }

    if (newColumn.SortOrder != oldColumn.SortOrder) {
        THROW_ERROR_EXCEPTION("Sort order mismatch for column %Qv: old %Qlv, new %Qlv",
            oldColumn.Name,
            oldColumn.SortOrder,
            newColumn.SortOrder);
    }

    if (newColumn.Expression != oldColumn.Expression) {
        THROW_ERROR_EXCEPTION("Expression mismatch for column %Qv: old %Qv, new %Qv",
            oldColumn.Name,
            oldColumn.Expression,
            newColumn.Expression);
    }

    if (oldColumn.Aggregate && oldColumn.Aggregate != newColumn.Aggregate) {
        THROW_ERROR_EXCEPTION("Aggregate mode mismatch for column %Qv: old %Qv, new %Qv",
            oldColumn.Name,
            oldColumn.Aggregate,
            newColumn.Aggregate);
    }

    if (oldColumn.SortOrder && oldColumn.Lock != newColumn.Lock) {
        THROW_ERROR_EXCEPTION("Lock mismatch for key column %Qv: old %Qv, new %Qv",
            oldColumn.Name,
            oldColumn.Lock,
            newColumn.Lock);
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Validates that all columns from the old schema are present in the new schema.
void ValidateColumnsNotRemoved(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    YCHECK(newSchema.GetStrict());
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.Columns().size(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        if (!newSchema.FindColumn(oldColumn.Name)) {
            THROW_ERROR_EXCEPTION("Cannot remove column %Qv from a strict schema",
                oldColumn.Name);
        }
    }
}

//! Validates that all columns from the new schema are present in the old schema.
void ValidateColumnsNotInserted(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    YCHECK(!oldSchema.GetStrict());
    for (int newColumnIndex = 0; newColumnIndex < newSchema.Columns().size(); ++newColumnIndex) {
        const auto& newColumn = newSchema.Columns()[newColumnIndex];
        if (!oldSchema.FindColumn(newColumn.Name)) {
            THROW_ERROR_EXCEPTION("Cannot insert a new column %Qv into non-strict schema",
                newColumn.Name);
        }
    }
}

//! Validates that for each column present in both #oldSchema and #newSchema, its declarations match each other.
//! Also validates that key columns positions are not changed.
void ValidateColumnsMatch(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.Columns().size(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        const auto* newColumnPtr = newSchema.FindColumn(oldColumn.Name);
        if (!newColumnPtr) {
            // We consider only columns present both in oldSchema and newSchema.
            continue;
        }
        const auto& newColumn = *newColumnPtr;
        ValidateColumnSchemaUpdate(oldColumn, newColumn);
        int newColumnIndex = newSchema.GetColumnIndex(newColumn);
        if (oldColumnIndex < oldSchema.GetKeyColumnCount()) {
            if (oldColumnIndex != newColumnIndex) {
                THROW_ERROR_EXCEPTION("Cannot change position of a key column %Qv: old %v, new %v",
                    oldColumn.Name,
                    oldColumnIndex,
                    newColumnIndex);
            }
        }
    }
}

//! Validates that there are no duplicates among the column names.
void ValidateColumnUniqueness(const TTableSchema& schema)
{
    yhash_set<Stroka> columnNames;
    for (const auto& column : schema.Columns()) {
        if (!columnNames.insert(column.Name).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in table schema",
                column.Name);
        }
    }
}

//! Validates that number of locks doesn't exceed #MaxColumnLockCount.
void ValidateLocks(const TTableSchema& schema)
{
    yhash_set<Stroka> lockNames;
    YCHECK(lockNames.insert(PrimaryLockName).second);
    for (const auto& column : schema.Columns()) {
        if (column.Lock) {
            lockNames.insert(*column.Lock);
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
        if (!schema.Columns()[index].SortOrder) {
            THROW_ERROR_EXCEPTION("Key columns must form a prefix of schema");
        }
    }
    // The fact that first GetKeyColumnCount() columns have SortOrder automatically
    // implies that the rest of columns don't have SortOrder, so we don't need to check it.
}

//! Validates computed columns.
/*!
 *  Validates that:
 *  - Computed column has to be key column.
 *  - Type of a computed column matches the type of its expression.
 *  - All referenced columns appear in schema, are key columns and are not computed.
 */
void ValidateComputedColumns(const TTableSchema& schema)
{
    // TODO(max42): Passing *this before the object is finally constructed
    // doesn't look like a good idea (although it works :) ). Get rid of this.

    for (int index = 0; index < schema.Columns().size(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Expression) {
            if (index >= schema.GetKeyColumnCount()) {
                THROW_ERROR_EXCEPTION("Non-key column %Qv can't be computed", columnSchema.Name);
            }
            yhash_set<Stroka> references;
            auto expr = PrepareExpression(columnSchema.Expression.Get(), schema, BuiltinTypeInferrersMap, &references);
            if (expr->Type != columnSchema.Type) {
                THROW_ERROR_EXCEPTION(
                    "Computed column %Qv type mismatch: declared type is %Qlv but expression type is %Qlv",
                    columnSchema.Name,
                    columnSchema.Type,
                    expr->Type);
            }

            for (const auto& ref : references) {
                const auto& refColumn = schema.GetColumnOrThrow(ref);
                if (!refColumn.SortOrder) {
                    THROW_ERROR_EXCEPTION("Computed column %Qv depends on a non-key column %Qv",
                        columnSchema.Name,
                        ref);
                }
                if (refColumn.Expression) {
                    THROW_ERROR_EXCEPTION("Computed column %Qv depends on a computed column %Qv",
                        columnSchema.Name,
                        ref);
                }
            }
        }
    }
}

//! Validates aggregated columns.
/*!
 *  Validates that:
 *  - Aggregated columns are non-key.
 *  - Aggregate function appears in a list of pre-defined aggregate functions.
 *  - Type of an aggregated column matches the type of an aggregate function.
 */
void ValidateAggregatedColumns(const TTableSchema& schema)
{
    for (int index = 0; index < schema.Columns().size(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Aggregate) {
            if (index < schema.GetKeyColumnCount()) {
                THROW_ERROR_EXCEPTION("Key column %Qv can't be aggregated", columnSchema.Name);
            }

            const auto& name = *columnSchema.Aggregate;
            if (auto descriptor = BuiltinTypeInferrersMap->GetFunction(name)->As<TAggregateTypeInferrer>()) {
                const auto& stateType = descriptor->InferStateType(columnSchema.Type, name, name);
                if (stateType != columnSchema.Type) {
                    THROW_ERROR_EXCEPTION("Aggregate function %Qv state type %Qv differs from column %Qv type %Qv",
                        columnSchema.Aggregate.Get(),
                        columnSchema.Type,
                        columnSchema.Name,
                        stateType);
                }
            } else {
                THROW_ERROR_EXCEPTION("Unknown aggregate function %Qv at column %Qv",
                    columnSchema.Aggregate.Get(),
                    columnSchema.Name);
            }
        }
    }
}

void ValidateTableSchema(const TTableSchema& schema)
{
    for (const auto& column : schema.Columns()) {
        ValidateColumnSchema(column);
    }
    ValidateColumnUniqueness(schema);
    ValidateLocks(schema);
    ValidateKeyColumnsFormPrefix(schema);
    ValidateComputedColumns(schema);
    ValidateAggregatedColumns(schema);
}

//! TODO(max42): document this functions somewhere (see also https://st.yandex-team.ru/YT-1433).
void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic,
    bool isTableEmpty)
{
    ValidateTableSchema(newSchema);

    if (isTableEmpty) {
        // Any valid schema is allowed to be set for an empty table.
        return;
    }

    if (oldSchema.GetKeyColumnCount() > 0 && newSchema.GetKeyColumnCount() == 0) {
        THROW_ERROR_EXCEPTION("Cannot change schema from sorted to unsorted");
    }
    if (oldSchema.GetKeyColumnCount() == 0 && newSchema.GetKeyColumnCount() > 0) {
        THROW_ERROR_EXCEPTION("Cannot change schema from unsorted to sorted");
    }

    if (isTableDynamic && !newSchema.GetStrict()) {
        THROW_ERROR_EXCEPTION("\"strict\" cannot be \"false\" for a dynamic table");
    }

    if (!oldSchema.GetStrict() && newSchema.GetStrict()) {
        THROW_ERROR_EXCEPTION("Changing \"strict\" from \"false\" to \"true\" is not allowed");
    }

    if (oldSchema.GetStrict() && !newSchema.GetStrict()) {
        if (oldSchema.Columns() != newSchema.Columns()) {
            THROW_ERROR_EXCEPTION("Changing columns is not allowed while changing \"strict\" from \"true\" to \"false\"");
        }
        return;
    }

    if (oldSchema.GetStrict()) {
        ValidateColumnsNotRemoved(oldSchema, newSchema);
    } else {
        ValidateColumnsNotInserted(oldSchema, newSchema);
    }
    ValidateColumnsMatch(oldSchema, newSchema);

    // We allow adding computed columns only on creation of the table.
    if (!oldSchema.Columns().empty() || !isTableEmpty) {
        for (const auto& newColumn : newSchema.Columns()) {
            if (!oldSchema.FindColumn(newColumn.Name)) {
                if (newColumn.Expression) {
                    THROW_ERROR_EXCEPTION("Cannot introduce a new computed column %Qv after creation",
                        newColumn.Name);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidatePivotKey(const TOwningKey& pivotKey, const TTableSchema& schema)
{
    if (pivotKey.GetCount() > schema.GetKeyColumnCount()) {
        THROW_ERROR_EXCEPTION("Pivot key must form a prefix of key");
    }

    for (int index = 0; index < pivotKey.GetCount(); ++index) {
        if (pivotKey[index].Type != schema.Columns()[index].Type) {
            THROW_ERROR_EXCEPTION(
                "Mismatched type of column %Qv in pivot key: expected %Qlv, found %Qlv",
                schema.Columns()[index].Name,
                schema.Columns()[index].Type,
                pivotKey[index].Type);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

EValueType GetCommonValueType(EValueType lhs, EValueType rhs) {
    if (lhs == EValueType::Null) {
        return rhs;
    } else if (rhs == EValueType::Null) {
        return lhs;
    } else if (lhs == rhs) {
        return lhs;
    } else {
        return EValueType::Any;
    }
}

TTableSchema InferInputSchema(const std::vector<TTableSchema>& schemas, bool discardKeyColumns)
{
    YCHECK(!schemas.empty());

    int commonKeyColumnPrefix = 0;
    if (!discardKeyColumns) {
        while (true) {
            if (commonKeyColumnPrefix >= schemas.front().GetKeyColumnCount()) {
                break;
            }
            const auto& keyColumnName = schemas.front().Columns()[commonKeyColumnPrefix].Name;
            bool mismatch = false;
            for (const auto& schema : schemas) {
                if (commonKeyColumnPrefix >= schema.GetKeyColumnCount() ||
                    schema.Columns()[commonKeyColumnPrefix].Name != keyColumnName)
                {
                    mismatch = true;
                    break;
                }
            }
            if (mismatch) {
                break;
            }
            ++commonKeyColumnPrefix;
        }
    }

    yhash_map<Stroka, TColumnSchema> nameToColumnSchema;
    std::vector<Stroka> columnNames;

    for (const auto& schema : schemas) {
        for (int columnIndex = 0; columnIndex < schema.Columns().size(); ++columnIndex) {
            auto column = schema.Columns()[columnIndex];
            if (columnIndex >= commonKeyColumnPrefix) {
                column = column.SetSortOrder(Null);
            }
            column = column
                .SetExpression(Null)
                .SetLock(Null);

            auto it = nameToColumnSchema.find(column.Name);
            if (it == nameToColumnSchema.end()) {
                nameToColumnSchema[column.Name] = column;
                columnNames.push_back(column.Name);
            } else {
                auto commonType = GetCommonValueType(it->second.Type, column.Type);
                column.Type = it->second.Type = commonType;
                if (it->second != column) {
                    THROW_ERROR_EXCEPTION(
                        "Conflict while merging schemas, column %Qs has two conflicting declarations",
                        column.Name)
                        << TErrorAttribute("first_column_schema", it->second)
                        << TErrorAttribute("second_column_schema", column);
                }
            }
        }
    }

    std::vector<TColumnSchema> columns;
    for (auto columnName : columnNames) {
        columns.push_back(nameToColumnSchema[columnName]);
    }

    bool strict = true;
    for (const auto& schema : schemas) {
        if (!schema.GetStrict()) {
            strict = false;
        }
    }

    return TTableSchema(columns, strict);
}

//! Validates that read schema is consistent with existing table schema.
/*! 
 *  Validates that:
 *  - If a column is present in both schemas, column types should be the same.
 *  - Either one of two possibilties holds:
 *    - #tableSchema.GetKeyColumns() is a prefix of #readSchema.GetKeyColumns()
 *    - #readSchema.GetKeyColumns() is a proper subset of #tableSchema.GetKeyColumns() and
 *      no extra key column in #readSchema appears in #tableSchema as a non-key column.
 */
void ValidateReadSchema(const TTableSchema& readSchema, const TTableSchema& tableSchema)
{
    for (int readColumnIndex = 0;
         readColumnIndex < static_cast<int>(tableSchema.Columns().size()); 
         ++readColumnIndex) {
        const auto& readColumn = readSchema.Columns()[readColumnIndex];
        const auto* tableColumnPtr = tableSchema.FindColumn(readColumn.Name);
        if (!tableColumnPtr) {
            continue;
        }

        // Validate column type consistency in two schemas.
        const auto& tableColumn = *tableColumnPtr;
        if (readColumn.Type != EValueType::Any &&
            tableColumn.Type != EValueType::Any &&
            readColumn.Type != tableColumn.Type) 
        {
            THROW_ERROR_EXCEPTION(
                "Mismatched type of column %Qv in read schema: expected %Qlv, found %Qlv",
                readColumn.Name,
                tableColumn.Type,
                readColumn.Type);
        }

        // Validate that order of key columns intersection hasn't been changed.
        int tableColumnIndex = tableSchema.GetColumnIndex(tableColumn);
        if (readColumnIndex < readSchema.GetKeyColumnCount() && 
            tableColumnIndex < readSchema.GetKeyColumnCount() &&
            readColumnIndex != tableColumnIndex) 
        {
            THROW_ERROR_EXCEPTION(
                "Key column %Qv position mismatch: its position is %v in table schema and %v in read schema",
                readColumn.Name,
                tableColumnIndex,
                readColumnIndex);
        }

        // Validate that a non-key column in tableSchema can't become a key column in readSchema.
        if (readColumnIndex < readSchema.GetKeyColumnCount() &&
            tableColumnIndex >= tableSchema.GetKeyColumnCount()) 
        {
            THROW_ERROR_EXCEPTION(
                "Column %Qv is declared as non-key in table schema and as a key in read schema",
                readColumn.Name);
        }
    }

    if (readSchema.GetKeyColumnCount() > tableSchema.GetKeyColumnCount() && !tableSchema.GetStrict()) {
        THROW_ERROR_EXCEPTION(
            "Table schema is not strict but read schema contains key column %Qv not present in table schema",
            readSchema.Columns()[tableSchema.GetKeyColumnCount()].Name);
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
