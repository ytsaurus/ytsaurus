#include "store.h"

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NLsm {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void Serialize(
    const TRowDigestUpcomingCompactionInfo& info,
    IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("reason").Value(info.Reason)
            .Item("timestamp").Value(info.Timestamp)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TStore::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Id_);
    Persist(context, Type_);
    Persist(context, StoreState_);
    Persist(context, CompressedDataSize_);
    Persist(context, UncompressedDataSize_);
    Persist(context, RowCount_);
    Persist(context, MinTimestamp_);
    Persist(context, MaxTimestamp_);
    Persist(context, FlushState_);
    Persist(context, LastFlushAttemptTimestamp_);
    Persist(context, DynamicMemoryUsage_);
    Persist(context, PreloadState_);
    Persist(context, CompactionState_);
    Persist(context, IsCompactable_);
    Persist(context, CreationTime_);
    Persist(context, LastCompactionTimestamp_);
    Persist(context, BackingStoreMemoryUsage_);
    Persist(context, MinKey_);
    Persist(context, UpperBoundKey_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
