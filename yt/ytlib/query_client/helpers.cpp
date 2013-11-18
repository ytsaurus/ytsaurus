#include "public.h"

#include <ytlib/new_table_client/schema.h>

#include <core/misc/protobuf_helpers.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NQueryClient {

typedef NVersionedTableClient::NProto::TTableSchemaExt TTableSchemaProto;
typedef NTableClient::NProto::TKeyColumnsExt TKeyColumnsProto;

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit)
{
    return NYT::FromProto<NObjectClient::TObjectId>(dataSplit.chunk_id());
}

TTableSchema GetTableSchemaFromDataSplit(const TDataSplit& dataSplit)
{
    auto tableSchemaProto = GetProtoExtension<TTableSchemaProto>(
        dataSplit.chunk_meta().extensions());
    return NYT::FromProto<TTableSchema>(tableSchemaProto);
}

TKeyColumns GetKeyColumnsFromDataSplit(const TDataSplit& dataSplit)
{
    auto keyColumnsProto = GetProtoExtension<TKeyColumnsProto>(
        dataSplit.chunk_meta().extensions());
    return NYT::FromProto<Stroka>(keyColumnsProto.names());
}

void SetObjectId(TDataSplit* dataSplit, const NObjectClient::TObjectId& objectId)
{
    ToProto(dataSplit->mutable_chunk_id(), objectId);
}

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema)
{
    TTableSchemaProto tableSchemaProto;
    ToProto(&tableSchemaProto, tableSchema);
    SetProtoExtension<TTableSchemaProto>(
        dataSplit->mutable_chunk_meta()->mutable_extensions(),
        tableSchemaProto);
}

void SetKeyColumns(TDataSplit* dataSplit, const TKeyColumns& keyColumns)
{
    TKeyColumnsProto keyColumnsProto;
    ToProto(keyColumnsProto.mutable_names(), keyColumns);
    SetProtoExtension<TKeyColumnsProto>(
        dataSplit->mutable_chunk_meta()->mutable_extensions(),
        keyColumnsProto);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

