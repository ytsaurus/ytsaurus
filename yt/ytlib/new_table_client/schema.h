#pragma once

#include "public.h"

#include <core/ytree/public.h>

#include <core/yson/public.h>

#include <core/misc/error.h>
#include <core/misc/nullable.h>
#include <core/misc/property.h>
#include <core/misc/serialize.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnSchema
{
    TColumnSchema();
    TColumnSchema(const Stroka& name, EValueType type);

    TColumnSchema(const TColumnSchema&) = default;
    TColumnSchema(TColumnSchema&&) = default;

    TColumnSchema& operator=(const TColumnSchema&) = default;
    TColumnSchema& operator=(TColumnSchema&&) = default;

    Stroka Name;
    EValueType Type;
};

void Serialize(const TColumnSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TColumnSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TColumnSchema* protoSchema, const TColumnSchema& schema);
void FromProto(TColumnSchema* schema, const NProto::TColumnSchema& protoSchema);

////////////////////////////////////////////////////////////////////////////////

class TTableSchema
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<TColumnSchema>, Columns);

public:
    TColumnSchema* FindColumn(const TStringBuf& name);
    const TColumnSchema* FindColumn(const TStringBuf& name) const;

    TColumnSchema& GetColumnOrThrow(const TStringBuf& name);
    const TColumnSchema& GetColumnOrThrow(const TStringBuf& name) const;

    int GetColumnIndex(const TColumnSchema& column) const;
    int GetColumnIndexOrThrow(const TStringBuf& name) const;

    TError CheckKeyColumns(const TKeyColumns& keyColumns) const;

    TTableSchema Filter(const TColumnFilter& columnFilter) const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

};

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema);
void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema);

bool operator == (const TColumnSchema& lhs, const TColumnSchema& rhs);
bool operator != (const TColumnSchema& lhs, const TColumnSchema& rhs);

bool operator == (const TTableSchema& lhs, const TTableSchema& rhs);
bool operator != (const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

void ValidateKeyColumns(const TKeyColumns& keyColumns);

void ValidateTableScheme(const TTableSchema& tableScheme);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT


// TODO(babenko): move to NVersionedTableClient after migration
// NB: Need to place this into NProto for ADL to work properly since TKeyColumns is std::vector.
namespace NYT {
namespace NTableClient {
namespace NProto {

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TKeyColumnsExt* protoKeyColumns, const TKeyColumns& keyColumns);
void FromProto(TKeyColumns* keyColumns, const NProto::TKeyColumnsExt& protoKeyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NProto
} // namespace NTableClient
} // namespace NYT
