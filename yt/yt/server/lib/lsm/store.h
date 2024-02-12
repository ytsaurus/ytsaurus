#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/ytlib/table_client/versioned_row_digest.h>

#include <yt/yt/client/table_client/key.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

struct TRowDigestUpcomingCompactionInfo
{
    EStoreCompactionReason Reason = EStoreCompactionReason::None;
    TInstant Timestamp;
};

void Serialize(
    const TRowDigestUpcomingCompactionInfo& info,
    NYson::IYsonConsumer* consumer);

struct TCompactionHints
{
    TRowDigestUpcomingCompactionInfo RowDigest;
    bool IsChunkViewTooNarrow = false;
};

////////////////////////////////////////////////////////////////////////////////

class TStore
{
public:
    // All stores.
    DEFINE_BYVAL_RW_PROPERTY(TStoreId, Id);
    DEFINE_BYVAL_RW_PROPERTY(EStoreType, Type);
    DEFINE_BYVAL_RW_PROPERTY(EStoreState, StoreState);

    DEFINE_BYVAL_RW_PROPERTY(i64, CompressedDataSize);
    DEFINE_BYVAL_RW_PROPERTY(i64, UncompressedDataSize);
    DEFINE_BYVAL_RW_PROPERTY(i64, RowCount);

    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, MinTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(TTimestamp, MaxTimestamp);

    DEFINE_BYVAL_RW_PROPERTY(TTablet*, Tablet);

    // Dynamic stores.
    DEFINE_BYVAL_RW_PROPERTY(EStoreFlushState, FlushState, EStoreFlushState::None);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastFlushAttemptTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(i64, DynamicMemoryUsage);

    // Chunk stores.
    DEFINE_BYVAL_RW_PROPERTY(EStorePreloadState, PreloadState, EStorePreloadState::Complete);
    DEFINE_BYVAL_RW_PROPERTY(EStoreCompactionState, CompactionState, EStoreCompactionState::None);
    DEFINE_BYVAL_RW_PROPERTY(bool, IsCompactable, false);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(TInstant, LastCompactionTimestamp);
    DEFINE_BYVAL_RW_PROPERTY(i64, BackingStoreMemoryUsage);

    // Sorted stores.
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TLegacyOwningKey, MinKey);
    DEFINE_BYREF_RW_PROPERTY(NTableClient::TLegacyOwningKey, UpperBoundKey);
    DEFINE_BYREF_RW_PROPERTY(TCompactionHints, CompactionHints);

    // Ordered stores.
    // Nothing here yet.

public:
    void Persist(const TStreamPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
