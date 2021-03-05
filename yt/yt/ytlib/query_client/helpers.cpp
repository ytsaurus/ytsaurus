#include "helpers.h"
#include "query.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NQueryClient {

using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;

using NChunkClient::TLegacyReadLimit;
using NTableClient::MinKey;
using NTableClient::MaxKey;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit)
{
    return FromProto<NObjectClient::TObjectId>(dataSplit.chunk_id());
}

NObjectClient::TObjectId GetTabletIdFromDataSplit(const TDataSplit& dataSplit)
{
    return FromProto<NTabletClient::TTabletId>(dataSplit.tablet_id());
}

NObjectClient::TCellId GetCellIdFromDataSplit(const TDataSplit& dataSplit)
{
    return FromProto<NObjectClient::TCellId>(dataSplit.cell_id());
}

TTableSchemaPtr GetTableSchemaFromDataSplit(const TDataSplit& dataSplit)
{
    return FromProto<TTableSchemaPtr>(GetProtoExtension<TTableSchemaExt>(
        dataSplit.chunk_meta().extensions()));
}

TLegacyOwningKey GetLowerBoundFromDataSplit(const TDataSplit& dataSplit)
{
    if (dataSplit.has_lower_limit()) {
        auto readLimit = FromProto<TLegacyReadLimit>(dataSplit.lower_limit());
        return readLimit.GetLegacyKey();
    } else {
        return MinKey();
    }
}

TLegacyOwningKey GetUpperBoundFromDataSplit(const TDataSplit& dataSplit)
{
    if (dataSplit.has_upper_limit()) {
        auto readLimit = FromProto<TLegacyReadLimit>(dataSplit.upper_limit());
        return readLimit.GetLegacyKey();
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

void SetObjectId(TDataSplit* dataSplit, NObjectClient::TObjectId objectId)
{
    ToProto(dataSplit->mutable_chunk_id(), objectId);
}

void SetTabletId(TDataSplit* dataSplit, NTabletClient::TTabletId tabletId)
{
    ToProto(dataSplit->mutable_tablet_id(), tabletId);
}

void SetCelllId(TDataSplit* dataSplit, NObjectClient::TCellId cellId)
{
    ToProto(dataSplit->mutable_cell_id(), cellId);
}

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema)
{
    SetProtoExtension(
        dataSplit->mutable_chunk_meta()->mutable_extensions(),
        ToProto<TTableSchemaExt>(tableSchema));
}

void SetLowerBound(TDataSplit* dataSplit, const TLegacyOwningKey & lowerBound)
{
    if (lowerBound == MinKey()) {
        dataSplit->clear_lower_limit();
        return;
    }
    TLegacyReadLimit readLimit;
    readLimit.SetLegacyKey(lowerBound);
    ToProto(dataSplit->mutable_lower_limit(), readLimit);
}

void SetUpperBound(TDataSplit* dataSplit, const TLegacyOwningKey & upperBound)
{
    if (upperBound == MaxKey()) {
        dataSplit->clear_upper_limit();
        return;
    }
    TLegacyReadLimit readLimit;
    readLimit.SetLegacyKey(upperBound);
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
    return QueryClientLogger.WithTag("FragmentId: %v", query->Id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

