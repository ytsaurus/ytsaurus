#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderSweeper
    : public TRefCounted
{
public:
    TChunkReaderSweeper(
        NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
        IInvokerPtr storageHeavyInvoker);

    //! Schedules calling #IChunk::TrySweepReader after a configured period of time
    //! (see #TDataNodeDynamicConfig::ChunkReaderRetentionTimeout).
    void ScheduleChunkReaderSweep(IChunkPtr chunk);

private:
    NClusterNode::TClusterNodeDynamicConfigManagerPtr DynamicConfigManager_;
    const NConcurrency::TPeriodicExecutorPtr ChunkReaderSweepExecutor_;

    struct TChunkReaderSweepEntry
    {
        IChunkPtr Chunk;
        TInstant Deadline;
    };

    TMpscStack<TChunkReaderSweepEntry> ChunkReaderSweepStack_;
    TRingQueue<TChunkReaderSweepEntry> ChunkReaderSweepQueue_;

    static constexpr auto ChunkReaderSweepPeriod = TDuration::Seconds(1);

    void OnChunkReaderSweep();
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderSweeper)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
