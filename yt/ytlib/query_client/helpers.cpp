#include "stdafx.h"
#include "public.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NQueryClient {

using namespace NChunkClient::NProto;
using namespace NVersionedTableClient::NProto;

using NChunkClient::TReadLimit;
using NVersionedTableClient::MinKey;
using NVersionedTableClient::MaxKey;

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit)
{
    return NYT::FromProto<NObjectClient::TObjectId>(dataSplit.chunk_id());
}

TTableSchema GetTableSchemaFromDataSplit(const TDataSplit& dataSplit)
{
    auto tableSchemaProto = GetProtoExtension<TTableSchemaExt>(
        dataSplit.chunk_meta().extensions());
    return NYT::FromProto<TTableSchema>(tableSchemaProto);
}

TKeyColumns GetKeyColumnsFromDataSplit(const TDataSplit& dataSplit)
{
    auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(
        dataSplit.chunk_meta().extensions());
    return FromProto<TKeyColumns>(keyColumnsExt);
}

TKey GetLowerBoundFromDataSplit(const TDataSplit& dataSplit)
{
    if (dataSplit.has_lower_limit()) {
        auto readLimit = FromProto<TReadLimit>(dataSplit.lower_limit());
        return readLimit.GetKey();
    } else {
        return MinKey();
    }
}

TKey GetUpperBoundFromDataSplit(const TDataSplit& dataSplit)
{
    if (dataSplit.has_upper_limit()) {
        auto readLimit = FromProto<TReadLimit>(dataSplit.upper_limit());
        return readLimit.GetKey();
    } else {
        return MaxKey();
    }
}

TKeyRange GetBothBoundsFromDataSplit(const TDataSplit& dataSplit)
{
    return std::make_pair(
        GetLowerBoundFromDataSplit(dataSplit),
        GetUpperBoundFromDataSplit(dataSplit));
}

TTimestamp GetTimestampFromDataSplit(const TDataSplit& dataSplit)
{
    return dataSplit.has_timestamp() ? dataSplit.timestamp() : NullTimestamp;
}

bool IsSorted(const TDataSplit& dataSplit)
{
    auto miscProto = FindProtoExtension<TMiscExt>(
        dataSplit.chunk_meta().extensions());
    return miscProto ? miscProto->sorted(): false;
}

void SetObjectId(TDataSplit* dataSplit, const NObjectClient::TObjectId& objectId)
{
    ToProto(dataSplit->mutable_chunk_id(), objectId);
}

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema)
{
    SetProtoExtension(
        dataSplit->mutable_chunk_meta()->mutable_extensions(),
        ToProto<TTableSchemaExt>(tableSchema));
}

void SetKeyColumns(TDataSplit* dataSplit, const TKeyColumns& keyColumns)
{
    SetProtoExtension(
        dataSplit->mutable_chunk_meta()->mutable_extensions(),
        ToProto<TKeyColumnsExt>(keyColumns));
}

void SetLowerBound(TDataSplit* dataSplit, const TKey& lowerBound)
{
    if (lowerBound == MinKey()) {
        dataSplit->clear_lower_limit();
        return;
    }
    TReadLimit readLimit;
    readLimit.SetKey(lowerBound);
    ToProto(dataSplit->mutable_lower_limit(), readLimit);
}

void SetUpperBound(TDataSplit* dataSplit, const TKey& upperBound)
{
    if (upperBound == MaxKey()) {
        dataSplit->clear_upper_limit();
        return;
    }
    TReadLimit readLimit;
    readLimit.SetKey(upperBound);
    ToProto(dataSplit->mutable_upper_limit(), readLimit);
}

void SetBothBounds(TDataSplit* dataSplit, const TKeyRange& keyRange)
{
    SetLowerBound(dataSplit, keyRange.first);
    SetUpperBound(dataSplit, keyRange.second);
}

void SetTimestamp(TDataSplit* dataSplit, TTimestamp timestamp)
{
    if (timestamp == NullTimestamp) {
        dataSplit->clear_timestamp();
    } else {
        dataSplit->set_timestamp(timestamp);
    }
}

void SetSorted(TDataSplit* dataSplit, bool isSorted)
{
    auto miscProto = FindProtoExtension<TMiscExt>(
        dataSplit->chunk_meta().extensions());
    if (!miscProto) {
        miscProto = TMiscExt();
    }
    miscProto->set_sorted(isSorted);
    SetProtoExtension<TMiscExt>(
        dataSplit->mutable_chunk_meta()->mutable_extensions(),
        *miscProto);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

