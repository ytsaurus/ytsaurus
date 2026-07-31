#pragma once

#include "output_store.h"

#include <yt/yt/flow/library/cpp/common/time_provider.h>

#include <yt/yt/flow/library/cpp/tables/public.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TCompactOutputStoreContext
    : public TOutputStoreContext
{
    NTables::ICompactPartitionOutputMessagesPtr CompactPartitionOutputMessagesTable;
    NTables::ICompactOutputMessagesPtr CompactOutputMessagesTable;
    ITimeProviderPtr TimeProvider;

    // Erase-row budget for the epoch transaction (each op — a mask update or a chunk erase — is one
    // YT row). YT caps a transaction at 100'000 modified rows and the epoch tx also carries writes +
    // offsets + state + timers + sinks, so keep well under; overflow drains via async transactions.
    int MaxEraseRowsPerEpochTransaction = 20'000;

    // Erase-row budget for each dedicated post-commit async transaction; margin under the 100'000 cap.
    int MaxEraseRowsPerAsyncTransaction = 90'000;
};

DEFINE_REFCOUNTED_TYPE(TCompactOutputStoreContext);

////////////////////////////////////////////////////////////////////////////////

// Chunk-based OutputStore against compact_output_messages/compact_partition_output_messages
// with processed_mask resume and seqno-encoded chunkId/position.
IOutputStorePtr CreateCompactOutputStore(TCompactOutputStoreContextPtr context, TDynamicOutputStoreSpecPtr dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
