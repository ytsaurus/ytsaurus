#include "schema.h"
#include "unversioned_row.h"

#include <yt/core/ytree/serialize.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/ytlib/table_client/chunk_meta.pb.h>

// TODO(sandello): Refine this dependencies.
// TODO(lukyan): Remove this dependencies.
#include <yt/ytlib/query_client/plan_fragment.h>
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/folding_profiler.h>
#include <yt/ytlib/query_client/function_registry.h>
#include <yt/ytlib/query_client/functions.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NYson;
using namespace NQueryClient;

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

        RegisterValidator([&] () {
            ValidateColumnSchema(*this);
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
    // TODO(babenko): we shouldn't be concerned with manual validation here
    wrapper.Validate();
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
    ValidateColumnSchema(*schema);
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema::TTableSchema()
    : Strict_(false)
{ }

TTableSchema::TTableSchema(const std::vector<TColumnSchema>& columns, bool strict)
    : Columns_(columns)
    , Strict_(strict)
{
    for (const auto& column : Columns_) {
        if (column.SortOrder)
            ++KeyColumnCount_;
    } 
    Validate();
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
        result.Columns_.push_back(Columns_[id]);
    }
    return result;
}

// TODO(max42): refactor this method so that it doesn't use keyColumns argument.
TTableSchema TTableSchema::TrimNonkeyColumns(const TKeyColumns& keyColumns) const
{
    std::vector<TColumnSchema> resultColumns;
    YCHECK(Columns_.size() >= keyColumns.size());
    for (int id = 0; id < keyColumns.size(); ++id) {
        YCHECK(Columns_[id].Name == keyColumns[id]);
        resultColumns.push_back(Columns_[id]);
    }
    return TTableSchema(resultColumns, Strict_);
}

TTableSchema TTableSchema::GetPrefix(int length) const 
{
    return TTableSchema(
        std::vector<TColumnSchema>(
            Columns_.begin(),
            Columns_.begin() + std::min(length, static_cast<int>(Columns_.size()))),
        Strict_);
}
  
void TTableSchema::AppendColumn(const TColumnSchema& column)
{
    ValidateColumnSchema(column);
    TTableSchema temp = *this;
    temp.Columns_.push_back(column);
    if (column.SortOrder) {
        ++temp.KeyColumnCount_;
    }
    temp.Validate();
    this->Swap(temp);
}

void TTableSchema::InsertColumn(int position, const TColumnSchema& column)
{
    ValidateColumnSchema(column);
    if (position < 0 || position > Columns_.size()) {
        THROW_ERROR_EXCEPTION("Position is invalid: %v (table contains %v columns)",
            position, Columns_.size());
    }
    TTableSchema temp = *this;
    temp.Columns_.insert(temp.Columns_.begin() + position, column);
    if (column.SortOrder) {
        ++temp.KeyColumnCount_;
    }
    temp.Validate();
    this->Swap(temp);
}
    
void TTableSchema::EraseColumn(int position)
{
    if (position < 0 || position > Columns_.size()) {
        THROW_ERROR_EXCEPTION("Position is invalid: %v (table contains %v columns)",
           position, Columns_.size()); 
    }
    TTableSchema temp = *this;
    if (Columns_[position].SortOrder) {
        --temp.KeyColumnCount_;
    }
    temp.Columns_.erase(temp.Columns_.begin() + position);
    temp.Validate();
    this->Swap(temp);
}
    
void TTableSchema::AlterColumn(int position, const TColumnSchema& column)
{
    ValidateColumnSchema(column);
    if (position < 0 || position > Columns_.size()) {
        THROW_ERROR_EXCEPTION("Position is invalid: %v (table contains %v columns)",
            position, Columns_.size());
    }
    TTableSchema temp = *this;
    if (temp.Columns_[position].SortOrder) {
        --temp.KeyColumnCount_;
    }
    temp.Columns_[position] = column;
    if (column.SortOrder) {
        ++temp.KeyColumnCount_;
    }
    temp.Validate();
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
    TTableSchema tableSchema;
    for (const auto& columnName : keyColumns) {
        tableSchema.Columns_.push_back(
            TColumnSchema(columnName, EValueType::Any)
                .SetSortOrder(ESortOrder::Ascending)); 
    }
    tableSchema.KeyColumnCount_ = keyColumns.size();
    tableSchema.Validate();
    return tableSchema;
}

void TTableSchema::Save(TStreamSaveContext& context) const
{
    NYT::Save(context, NYT::ToProto<NTableClient::NProto::TTableSchemaExt>(*this));
}

void TTableSchema::Load(TStreamLoadContext& context)
{
    NTableClient::NProto::TTableSchemaExt protoSchema;
    NYT::Load(context, protoSchema);
    *this = NYT::FromProto<TTableSchema>(protoSchema);
}

void TTableSchema::Swap(TTableSchema& other)
{
    Columns_.swap(other.Columns_);
    std::swap(Strict_, other.Strict_);
    std::swap(KeyColumnCount_, other.KeyColumnCount_);
}

//! Validates that there are no duplicates among the column names.
void TTableSchema::ValidateColumnUniqueness()
{
    yhash_set<Stroka> columnNames;
    for (const auto& column : Columns_) {
        if (!columnNames.insert(column.Name).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in table schema",
                column.Name);
        }
    }
}

//! Validates that number of locks doesn't exceed MaxColumnLockCount.
void TTableSchema::ValidateLocks()
{
    yhash_set<Stroka> lockNames;
    YCHECK(lockNames.insert(PrimaryLockName).second);
    for (const auto& column : Columns_) {
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
void TTableSchema::ValidateKeyColumnsFormPrefix()
{
    for (int index = 0; index < KeyColumnCount_; ++index)
    {
        if (!Columns_[index].SortOrder) {
            THROW_ERROR_EXCEPTION("Key columns must form a prefix of schema");
        }
    }
    // The fact that first GetKeyColumnCount() columns have SortOrder automatically
    // implies that the rest of columns don't have SortOrder, so we don't need to check it.
}

//! Validates computed columns.
/*!
 *  Checks that:
 *  - Computed column has to be key column.
 *  - Type of a computed column matches the type of its expression.
 *  - All referenced columns appear in schema, are key columns and are not computed.
 */
void TTableSchema::ValidateComputedColumns()
{
    // TODO(max42): Passing *this before the object is finally constructed
    // doesn't look like a good idea (although it works :) ). Get rid of this.

    for (int index = 0; index < Columns_.size(); ++index) {
        const auto& columnSchema = Columns_[index];
        if (columnSchema.Expression) {
            if (index >= KeyColumnCount_) {
                THROW_ERROR_EXCEPTION("Non-key column %Qv can't be computed", columnSchema.Name);
            }
            auto functionRegistry = CreateBuiltinFunctionRegistry();
            auto expr = PrepareExpression(columnSchema.Expression.Get(), *this, functionRegistry);
            if (expr->Type != columnSchema.Type) {
                THROW_ERROR_EXCEPTION("Computed column %Qv type mismatch: declared type is %Qlv but expression type is %Qlv",
                    columnSchema.Name,
                    columnSchema.Type,
                    expr->Type);
            }

            yhash_set<Stroka> references;
            Profile(expr, *this, nullptr, nullptr, &references, nullptr, functionRegistry);
            for (const auto& ref : references) {
                const auto& refColumn = GetColumnOrThrow(ref);
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
void TTableSchema::ValidateAggregatedColumns()
{
    auto functionRegistry = CreateBuiltinFunctionRegistry();
    for (int index = 0; index < Columns_.size(); ++index) {
        const auto& columnSchema = Columns_[index];
        if (columnSchema.Aggregate) {
            if (index < KeyColumnCount_) {
                THROW_ERROR_EXCEPTION("Key column %Qv can't be aggregated", columnSchema.Name);
            }
            if (auto descriptor = functionRegistry->FindAggregateFunction(columnSchema.Aggregate.Get())) {
                const auto& stateType = descriptor->GetStateType(columnSchema.Type);
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

//! TODO(max42): document this functions somewhere (see also https://st.yandex-team.ru/YT-1433).
void TTableSchema::Validate()
{    
    for (const auto& column : Columns_) {
        ValidateColumnSchema(column);
    }
    ValidateColumnUniqueness();
    ValidateLocks();
    ValidateKeyColumnsFormPrefix();
    ValidateComputedColumns();
    ValidateAggregatedColumns();
}

////////////////////////////////////////////////////////////////////////////////

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
    std::vector<TColumnSchema> columns = ConvertTo<std::vector<TColumnSchema>>(node);
    schema = TTableSchema(columns, node->Attributes().Get<bool>("strict", true));
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema)
{
    NYT::ToProto(protoSchema->mutable_columns(), schema.Columns());
    protoSchema->set_strict(schema.GetStrict());
}

void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema)
{
    *schema = TTableSchema(
        NYT::FromProto<TColumnSchema>(protoSchema.columns()), 
        protoSchema.has_strict() ? protoSchema.strict() : true);
}

void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& protoKeyColumns)
{
    std::vector<TColumnSchema> columns = NYT::FromProto<TColumnSchema>(protoSchema.columns());
    for (int columnIndex = 0; columnIndex < protoKeyColumns.names_size(); ++columnIndex) {
        auto& columnSchema = columns[columnIndex];
        YCHECK(columnSchema.Name == protoKeyColumns.names(columnIndex));
        YCHECK(!columnSchema.SortOrder);
        columnSchema.SortOrder = ESortOrder::Ascending;
    }
    *schema = TTableSchema(
        NYT::FromProto<TColumnSchema>(protoSchema.columns()), 
        protoSchema.has_strict() ? protoSchema.strict() : true);
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return lhs.Name == rhs.Name 
        && lhs.Type == rhs.Type
        && lhs.SortOrder == rhs.SortOrder
        && lhs.Aggregate == rhs.Aggregate
        && lhs.Expression == rhs.Expression;
}

bool operator != (const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const TTableSchema& lhs, const TTableSchema& rhs)
{
    return lhs.Columns() == rhs.Columns() && lhs.GetStrict() == rhs.GetStrict();
}

bool operator != (const TTableSchema& lhs, const TTableSchema& rhs)
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

        if (columnSchema.Name.substr(0, SystemColumnNamePrefix.size()) == SystemColumnNamePrefix) {
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
            if (columnSchema.SortOrder) {
                THROW_ERROR_EXCEPTION("Column lock cannot be set on a key column");
            }
            if (columnSchema.Lock->size() > MaxColumnLockLength) {
                THROW_ERROR_EXCEPTION("Column lock name is longer than maximum allowed: %v > %v",
                    columnSchema.Lock->size(),
                    MaxColumnLockLength);
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

void ValidateDynamicTableConstraints(const TTableSchema& schema)
{
    if (!schema.GetStrict()) {
        THROW_ERROR_EXCEPTION("Table schema must be strict");
    }

    if (schema.GetKeyColumnCount() == 0) {
        THROW_ERROR_EXCEPTION("There must be at least one key column");
    }

    if (schema.GetKeyColumnCount() == schema.Columns().size()) {
        THROW_ERROR_EXCEPTION("There must be at least one non-key column");
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

//! Validates that all columns from the new schema are presented in the old schema.
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

//! Validates that for each column presented in both oldSchema and newSchema, its declarations match each other.
//! Also validates that key columns positions are not changed.
void ValidateColumnsMatch(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.Columns().size(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        const auto* newColumnPtr = newSchema.FindColumn(oldColumn.Name);
        if (!newColumnPtr) {
            // We consider only columns presented both in oldSchema and newSchema.
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

//! TODO(max42): document this functions somewhere (see also https://st.yandex-team.ru/YT-1433).
void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic,
    bool isTableEmpty)
{ 
    if (isTableEmpty) {
        // Any valid schema is allowed to be set for an empty table.
        return;
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

    if (!oldSchema.GetStrict()) {
        ValidateColumnsNotInserted(oldSchema, newSchema);
    } else {
        ValidateColumnsNotRemoved(oldSchema, newSchema);
    }
    ValidateColumnsMatch(oldSchema, newSchema);
    
    // We allow adding computed columns only on creation of the table.
    if (!oldSchema.Columns().empty() || !isTableEmpty) {
        for (const auto& newColumn : newSchema.Columns()) {
            if (!oldSchema.FindColumn(newColumn.Name)) {
                if (newColumn.Expression) {
                    THROW_ERROR_EXCEPTION("New computed column %Qv",
                        newColumn.Name);
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidatePivotKey(const TOwningKey& pivotKey, const TTableSchema& schema, int keyColumnCount)
{
    if (pivotKey.GetCount() > keyColumnCount) {
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

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns)
{
    NYT::ToProto(protoKeyColumns->mutable_names(), keyColumns);
}

void FromProto(TKeyColumns* keyColumns, const TKeyColumnsExt& protoKeyColumns)
{
    *keyColumns = NYT::FromProto<Stroka>(protoKeyColumns.names());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
