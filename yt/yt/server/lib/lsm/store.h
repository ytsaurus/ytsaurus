#pragma once

#include <yt/yt/client/table_client/key.h>

#include "public.h"

namespace NYT::NLsm {

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

    // Ordered stores.
    // Nothing here yet.

public:
    bool IsOrdered() const;
    bool IsSorted() const;
    bool IsDynamic() const;
    bool IsChunk() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
