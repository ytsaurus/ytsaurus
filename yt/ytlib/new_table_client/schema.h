#pragma once

#include "public.h"

#include <core/ytree/public.h>

#include <core/yson/public.h>

#include <core/misc/nullable.h>
#include <core/misc/property.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

// For serialization.
namespace NProto {

class TColumnSchema;
class TTableSchemaExt;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TColumnSchema
{
    TColumnSchema();
    TColumnSchema(const Stroka& name, EValueType type);

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

};

void Serialize(const TTableSchema& schema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& schema, NYTree::INodePtr node);

void ToProto(NProto::TTableSchemaExt* protoSchema, const TTableSchema& schema);
void FromProto(TTableSchema* schema, const NProto::TTableSchemaExt& protoSchema);

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs);
bool operator==(const TTableSchema& lhs, const TTableSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
