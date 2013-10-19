#pragma once

#include "public.h"

#include <core/ytree/public.h>
#include <core/yson/public.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnSchema
{
    Stroka Name;
    EColumnType Type;
};

struct TTableSchema
{
    std::vector<TColumnSchema> Columns;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Maybe store just as proto?
void Serialize(const TColumnSchema& columnSchema, NYson::IYsonConsumer* consumer);
void Deserialize(TColumnSchema& columnSchema, NYTree::INodePtr node);
namespace NProto { class TColumnSchema; }
void FromProto(TColumnSchema* columnSchema, const NProto::TColumnSchema& protoColumnSchema);
void ToProto(NProto::TColumnSchema* protoColumnSchema, const TColumnSchema& columnSchema);

// TODO(sandello): Maybe store just as proto?
void Serialize(const TTableSchema& tableSchema, NYson::IYsonConsumer* consumer);
void Deserialize(TTableSchema& tableSchema, NYTree::INodePtr node);
namespace NProto { class TTableSchema; }
void FromProto(TTableSchema* tableSchema, const NProto::TTableSchema& protoTableSchema);
void ToProto(NProto::TTableSchema* protoTableSchema, const TTableSchema& tableSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT

