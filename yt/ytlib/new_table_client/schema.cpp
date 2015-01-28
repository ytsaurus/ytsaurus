#include "stdafx.h"
#include "schema.h"
#include "unversioned_row.h"

#include <core/ytree/serialize.h>
#include <core/ytree/convert.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/new_table_client/chunk_meta.pb.h>
#include <ytlib/table_client/table_chunk_meta.pb.h> // TODO(babenko): remove after migration

namespace NYT {
namespace NVersionedTableClient {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TColumnSchema::TColumnSchema()
    : Type(EValueType::Null)
{ }

TColumnSchema::TColumnSchema(
    const Stroka& name,
    EValueType type,
    const TNullable<Stroka>& lock,
    const TNullable<Stroka>& expression)
    : Name(name)
    , Type(type)
    , Lock(lock)
    , Expression(expression)
{ }

struct TSerializableColumnSchema
    : public TYsonSerializableLite
    , public TColumnSchema
{
    TSerializableColumnSchema()
    {
        RegisterAll();
    }

    explicit TSerializableColumnSchema(const TColumnSchema& other)
        : TColumnSchema(other)
    {
        RegisterAll();
    }

    void RegisterAll()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("type", Type);
        RegisterParameter("lock", Lock)
            .Default();
        RegisterParameter("expression", Expression)
            .Default();

        RegisterValidator([&] () {
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
    TSerializableColumnSchema wrapper(schema);
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
}

void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema)
{
    schema->Name = protoSchema.name();
    schema->Type = EValueType(protoSchema.type());
    schema->Lock = protoSchema.has_lock() ? MakeNullable(protoSchema.lock()) : Null;
    schema->Expression = protoSchema.has_expression() ? MakeNullable(protoSchema.expression()) : Null;
}

////////////////////////////////////////////////////////////////////////////////

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

void TTableSchema::Save(TStreamSaveContext& context) const
{
    NYT::Save(context, NYT::ToProto<NVersionedTableClient::NProto::TTableSchemaExt>(*this));
}

void TTableSchema::Load(TStreamLoadContext& context)
{
    NVersionedTableClient::NProto::TTableSchemaExt protoSchema;
    NYT::Load(context, protoSchema);
    *this = NYT::FromProto<TTableSchema>(protoSchema);
}

////////////////////////////////////////////////////////////////////////////////


void Serialize(const TTableSchema& schema, IYsonConsumer* consumer)
{
    NYTree::Serialize(schema.Columns(), consumer);
}

void Deserialize(TTableSchema& schema, INodePtr node)
{
    NYTree::Deserialize(schema.Columns(), node);
    ValidateTableSchema(schema);
}

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema)
{
    NYT::ToProto(protoSchema->mutable_columns(), schema.Columns());
}

void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema)
{
    schema->Columns() = NYT::FromProto<TColumnSchema>(protoSchema.columns());
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

void ValidateTableSchema(const TTableSchema& schema)
{
    // Check for duplicate column names.
    // Check lock groups count.
    yhash_set<Stroka> columnNames;
    yhash_set<Stroka> lockNames;
    YCHECK(lockNames.insert(PrimaryLockName).second);
    for (const auto& column : schema.Columns()) {
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
        if (columnSchema.Lock) {
            THROW_ERROR_EXCEPTION("Key column %Qv cannot have a lock",
                columnSchema.Name);
        }
    }

    if (schema.Columns().size() == keyColumns.size()) {
        THROW_ERROR_EXCEPTION("Schema must contains at least one non-key column");;
    }

    //FIXME(savrus) validate auto keys
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

} // namespace NVersionedTableClient
} // namespace NYT
