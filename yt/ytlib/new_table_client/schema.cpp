#include "public.h"
#include "schema.h"

#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <core/ytree/convert.h>
#include <core/ytree/node.h>

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//void Serialize(const TColumnSchema& columnSchema, NYson::IYsonConsumer* consumer);

void Deserialize(TColumnSchema& columnSchema, INodePtr node)
{
    columnSchema.Name = ConvertTo<Stroka>(node->AsMap()->GetChild("name"));
    columnSchema.Type = ConvertTo<EColumnType>(node->AsMap()->GetChild("type"));
}

void FromProto(TColumnSchema* columnSchema, const NProto::TColumnSchema& protoColumnSchema)
{
    columnSchema->Name = protoColumnSchema.name();
    columnSchema->Type = EColumnType(protoColumnSchema.type());
}

void ToProto(NProto::TColumnSchema* protoColumnSchema, const TColumnSchema& columnSchema)
{
    protoColumnSchema->set_name(columnSchema.Name);
    protoColumnSchema->set_type(columnSchema.Type);
}

//void Serialize(const TTableSchema& tableSchema, NYson::IYsonConsumer* consumer);

void Deserialize(TTableSchema& tableSchema, INodePtr node)
{
    tableSchema.Columns = ConvertTo<std::vector<TColumnSchema>>(node);
}

void FromProto(TTableSchema* tableSchema, const NProto::TTableSchema& protoTableSchema)
{
    tableSchema->Columns = NYT::FromProto<TColumnSchema>(protoTableSchema.columns());
}

void ToProto(NProto::TTableSchema* protoTableSchema, const TTableSchema& tableSchema)
{
    NYT::ToProto(protoTableSchema->mutable_columns(), tableSchema.Columns);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT


