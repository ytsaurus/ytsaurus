#pragma once

#include "public.h"

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NHiveClient {

////////////////////////////////////////////////////////////////////////////////

struct TTimestampMap
{
    SmallVector<std::pair<NObjectClient::TCellTag, NTransactionClient::TTimestamp>, 4> Timestamps;

    TTimestamp GetTimestamp(NObjectClient::TCellTag) const;

    void Persist(const TStreamPersistenceContext& context);
};

void ToProto(NProto::TTimestampMap* protoMap, const TTimestampMap& map);
void FromProto(TTimestampMap* map, const NProto::TTimestampMap& protoMap);

void FormatValue(TStringBuilder* builder, const TTimestampMap& map, const TStringBuf& spec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveClient
} // namespace NYT
