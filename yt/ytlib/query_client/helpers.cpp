#include "stdafx.h"
#include "public.h"

#include <core/misc/protobuf_helpers.h>

#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/table_client/schema.h>
#include <ytlib/table_client/unversioned_row.h>
#include <ytlib/table_client/chunk_meta_extensions.h>

namespace NYT {
namespace NQueryClient {

using namespace NChunkClient::NProto;
using namespace NTableClient::NProto;

using NChunkClient::TReadLimit;
using NTableClient::MinKey;
using NTableClient::MaxKey;

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

TOwningKey GetLowerBoundFromDataSplit(const TDataSplit& dataSplit)
{
    if (dataSplit.has_lower_limit()) {
        auto readLimit = FromProto<TReadLimit>(dataSplit.lower_limit());
        return readLimit.GetKey();
    } else {
        return MinKey();
    }
}

TOwningKey GetUpperBoundFromDataSplit(const TDataSplit& dataSplit)
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

void SetLowerBound(TDataSplit* dataSplit, const TOwningKey & lowerBound)
{
    if (lowerBound == MinKey()) {
        dataSplit->clear_lower_limit();
        return;
    }
    TReadLimit readLimit;
    readLimit.SetKey(lowerBound);
    ToProto(dataSplit->mutable_lower_limit(), readLimit);
}

void SetUpperBound(TDataSplit* dataSplit, const TOwningKey & upperBound)
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

TKeyColumns TableSchemaToKeyColumns(const TTableSchema& schema, size_t keySize)
{
    TKeyColumns keyColumns;
    keySize = std::min(keySize, schema.Columns().size());
    for (size_t i = 0; i < keySize; ++ i) {
        keyColumns.push_back(schema.Columns()[i].Name);
    }
    return keyColumns;
}

//! Computes key index for a given column name.
int ColumnNameToKeyPartIndex(const TKeyColumns& keyColumns, const Stroka& columnName)
{
    for (int index = 0; index < keyColumns.size(); ++index) {
        if (keyColumns[index] == columnName) {
            return index;
        }
    }
    return -1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

