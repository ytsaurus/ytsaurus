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

void ValidateColumnSchema(const TColumnSchema& columnSchema)
{
    try {
        if (columnSchema.Name.empty()) {
            THROW_ERROR_EXCEPTION("Column name cannot be empty");
        }

        if (columnSchema.Name.size() > MaxColumnNameLength) {
            THROW_ERROR_EXCEPTION("Column name is longer than maximum allowed: %v > %v", 
                columnSchema.Name.size(),
                MaxColumnNameLength);
        }

        if (columnSchema.Lock) {
            if (columnSchema.Lock->empty()) {
                THROW_ERROR_EXCEPTION("Lock should either be unset or be non-empty");
            }
            if (columnSchema.Lock->size() > MaxColumnLockLength) {
                THROW_ERROR_EXCEPTION("Column lock is longer than maximum allowed: %v > %v",
                    columnSchema.Lock->size(),
                    MaxColumnLockLength);
            }
        } 
    
        ValidateSchemaValueType(columnSchema.Type);

        if (columnSchema.Expression && !columnSchema.SortOrder) {
            THROW_ERROR_EXCEPTION("Non-key column can't be computed");
        }

        if (columnSchema.Aggregate && columnSchema.SortOrder) {
            THROW_ERROR_EXCEPTION("Key column can't be aggregated");
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating schema of a column %Qv",
            columnSchema.Name)
            << ex;
    }
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

TColumnSchema* TTableSchema::FindColumn(const TStringBuf& name)
{
    for (auto& column : Columns_) {
        if (column.Name == name) {
            return &column;
        }
    }
    return nullptr;
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

TColumnSchema& TTableSchema::GetColumnOrThrow(const TStringBuf& name)
{
    auto* column = FindColumn(name);
    if (!column) {
        THROW_ERROR_EXCEPTION("Missing schema column %Qv", name);
    }
    return *column;
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
        result.Columns().push_back(Columns_[id]);
    }
    return result;
}

TTableSchema TTableSchema::TrimNonkeyColumns(const TKeyColumns& keyColumns) const
{
    TTableSchema result;
    YCHECK(Columns_.size() >= keyColumns.size());
    for (int id = 0; id < keyColumns.size(); ++id) {
        YCHECK(Columns_[id].Name == keyColumns[id]);
        result.Columns().push_back(Columns_[id]);
    }
    return result;
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
    return !Columns_.empty() && Columns_.front().SortOrder;
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
    int keyColumnCount = 0;
    while (keyColumnCount < Columns().size() && Columns()[keyColumnCount].SortOrder) {
        ++keyColumnCount;
    }
    return keyColumnCount;
}
    
TTableSchema TTableSchema::FromKeyColumns(const TKeyColumns& keyColumns)
{
    TTableSchema tableSchema;
    tableSchema.Columns().clear();
    tableSchema.SetStrict(false);
    for (const auto& columnName : keyColumns) {
        tableSchema.Columns().push_back(
            TColumnSchema(columnName, EValueType::Any)
                .SetSortOrder(ESortOrder::Ascending)); 
    }
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
    NYTree::Deserialize(schema.Columns(), node);
    schema.SetStrict(node->Attributes().Get<bool>("strict", true));
    ValidateTableSchema(schema);
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema)
{
    NYT::ToProto(protoSchema->mutable_columns(), schema.Columns());
    protoSchema->set_strict(schema.GetStrict());
}

void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema)
{
    schema->Columns() = NYT::FromProto<TColumnSchema>(protoSchema.columns());
    schema->SetStrict(protoSchema.has_strict() ? protoSchema.strict() : true);
}

void FromProto(
    TTableSchema* schema,
    const NProto::TTableSchemaExt& protoSchema,
    const NProto::TKeyColumnsExt& protoKeyColumns)
{
    FromProto(schema, protoSchema);
    YCHECK(!schema->IsSorted());

    for (int columnIndex = 0; columnIndex < protoKeyColumns.names_size(); ++columnIndex) {
        auto& columnSchema = schema->Columns()[columnIndex];
        YCHECK(columnSchema.Name == protoKeyColumns.names(columnIndex));
        columnSchema.SortOrder = ESortOrder::Ascending;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return lhs.Name == rhs.Name && lhs.Type == rhs.Type;
}

bool operator != (const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return !(lhs == rhs);
}

bool operator == (const TTableSchema& lhs, const TTableSchema& rhs)
{
    return lhs.Columns() == rhs.Columns();
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

void ValidateTableSchema(const TTableSchema& schema)
{
    // Check for duplicate column names.
    // Check lock groups count.
    yhash_set<Stroka> columnNames;
    yhash_set<Stroka> lockNames;
    YCHECK(lockNames.insert(PrimaryLockName).second);
    for (const auto& column : schema.Columns()) {
        ValidateColumnSchema(column);

        if (!columnNames.insert(column.Name).second) {
            THROW_ERROR_EXCEPTION("Duplicate column name %Qv in table schema",
                column.Name);
        }
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

void ValidateTableSchemaAndKeyColumns(const TTableSchema& schema, const TKeyColumns& keyColumns)
{
    ValidateTableSchema(schema);
    ValidateKeyColumns(keyColumns);

    if (schema.Columns().size() < keyColumns.size()) {
        THROW_ERROR_EXCEPTION("Key columns must form a prefix of schema");;
    }

    for (int index = 0; index < static_cast<int>(keyColumns.size()); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Name != keyColumns[index]) {
            THROW_ERROR_EXCEPTION("Key columns must form a prefix of schema");
        }
    }

    if (schema.Columns().size() == keyColumns.size()) {
        THROW_ERROR_EXCEPTION("Schema must contains at least one non-key column");;
    }

    auto functionRegistry = CreateBuiltinFunctionRegistry();

    // Validate computed columns.
    for (int index = 0; index < schema.Columns().size(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Expression) {
            YCHECK(index < keyColumns.size());
            auto functionRegistry = CreateBuiltinFunctionRegistry();
            auto expr = PrepareExpression(columnSchema.Expression.Get(), schema, functionRegistry);
            if (expr->Type != columnSchema.Type) {
                THROW_ERROR_EXCEPTION("Computed column %Qv type mismatch: declared type is %Qlv but expression type is %Qlv",
                    columnSchema.Name,
                    columnSchema.Type,
                    expr->Type);
            }

            yhash_set<Stroka> references;
            Profile(expr, schema, nullptr, nullptr, &references, nullptr, functionRegistry);
            for (const auto& ref : references) {
                if (schema.GetColumnIndexOrThrow(ref) >= keyColumns.size()) {
                    THROW_ERROR_EXCEPTION("Computed column %Qv depends on a non-key column %Qv",
                        columnSchema.Name,
                        ref);
                }
                if (schema.GetColumnOrThrow(ref).Expression) {
                    THROW_ERROR_EXCEPTION("Computed column %Qv depends on computed column %Qv",
                        columnSchema.Name,
                        ref);
                }
            }
        }

        if (columnSchema.Aggregate) {
            YCHECK(index >= keyColumns.size());    
            if (auto descriptor = functionRegistry->FindAggregateFunction(columnSchema.Aggregate.Get())) {
                descriptor->GetStateType(columnSchema.Type);
            } else {
                THROW_ERROR_EXCEPTION("Unknown aggregate function %Qv at column %Qv",
                    columnSchema.Aggregate.Get(),
                    columnSchema.Name);
            }
        }
    }
}

void ValidateTableSchemaUpdate(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    ValidateTableSchema(newSchema);

    for (const auto& oldColumn : oldSchema.Columns()) {
        const auto& newColumn = newSchema.GetColumnOrThrow(oldColumn.Name);

        if (newColumn.Type != oldColumn.Type) {
            THROW_ERROR_EXCEPTION("Type mismatch for column %Qv: expected %Qv but got %Qv",
                oldColumn.Name,
                oldColumn.Type,
                newColumn.Type);
        }

        if (newColumn.Expression != oldColumn.Expression) {
            THROW_ERROR_EXCEPTION("Expression mismatch for column %Qv: expected %Qv but got %Qv",
                oldColumn.Name,
                oldColumn.Expression,
                newColumn.Expression);
        }

        //FIXME(savrus) enable when aggregates merged
#if 0
        if (oldColumn.Aggregate && oldColumn.Aggregate != newColumn.Aggregate) {
            THROW_ERROR_EXCEPTION("Aggregate mismatch for column %Qv: expected %Qv but got %Qv",
                oldColumn.Name,
                oldColumn.Aggregate,
                newColumn.Aggregate);
        }
#endif
    }

    // We allow adding computed columns only on creation of the table.
    if (!oldSchema.Columns().empty()) {
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
