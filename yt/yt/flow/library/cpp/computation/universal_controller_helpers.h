#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/key.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! New executing partitions must not produce output streams
//! which are not completely processed by overlapping interrupting partitions.
//! This class memorizes interrupting partitions for one computation
//! and computes blocked streams for executing partitions.
class TBlockedStreamComputer final
{
public:
    TBlockedStreamComputer(
        const TFlowViewPtr& flowView,
        const THashSet<TPartitionId>& interruptingPartitions,
        const NLogging::TLogger& logger);

    void AddInterruptingPartition(const TPartitionId& partitionId);

    THashSet<TStreamId> GetBlockedStreams(const TKey& lower, const TKey& upper) const;

    THashSet<TStreamId> GetBlockedStreams(const TKey& sourceKey) const;

protected:
    NLogging::TLogger Logger;

private:
    friend class TBlockedStreamComputerTest;

    struct TRangesMapValue
    {
        TKey Upper;
        THashSet<TStreamId> BlockingStreams;
    };

    const TFlowViewPtr FlowView_;
    // For ranges. Schema: {lowerKey: (upperKey, blockingStreams), ...}.
    std::map<TKey, TRangesMapValue> BlockingRanges_;
    // For source keys. Schema: {sourceKey: blockingStreams, ...}.
    THashMap<TKey, THashSet<TStreamId>> BlockingKeys_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
