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
};

DEFINE_REFCOUNTED_TYPE(TCompactOutputStoreContext);

////////////////////////////////////////////////////////////////////////////////

// Chunk-based OutputStore against compact_output_messages/compact_partition_output_messages
// with processed_mask resume and seqno-encoded chunkId/position.
IOutputStorePtr CreateCompactOutputStore(TCompactOutputStoreContextPtr context, TDynamicOutputStoreSpecPtr dynamicSpec);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
