#pragma once

#include "public.h"

#include <yt/yt/ytlib/sequoia_client/proto/transaction_client.pb.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/tablet_client/public.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

struct TLockedRowInfo
{
    NTableClient::TLockMask LockMask;

    NTabletClient::TTabletId TabletId;
    NTabletClient::TTabletCellId TabletCellId;
};

using TTableWriteSet = THashMap<NTableClient::TLegacyKey, TLockedRowInfo>;
using TWriteSet = TEnumIndexedVector<ESequoiaTable, TTableWriteSet>;

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TWriteSet* protoWriteSet, const TWriteSet& writeSet);

void FromProto(
    TWriteSet* writeSet,
    const NProto::TWriteSet& protoWriteSet,
    const NTableClient::TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
