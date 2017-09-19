#include "helpers.h"
#include "query.h"
#include "private.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NQueryClient {

using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;

using NChunkClient::TReadLimit;
using NTableClient::MinKey;
using NTableClient::MaxKey;

using NYT::FromProto;
using NYT::ToProto;

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

NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query)
{
    return NLogging::TLogger(QueryClientLogger)
        .AddTag("FragmentId: %v", query->Id);
}

size_t GetSignificantWidth(TRow row)
{
    auto valueIt = row.Begin();
    while (valueIt != row.End() && !IsSentinelType(valueIt->Type)) {
        ++valueIt;
    }
    return std::distance(row.Begin(), valueIt);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

