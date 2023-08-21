#include "chunk_reader_sweeper.h"

#include "chunk.h"

#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ring_queue.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NClusterNode;

////////////////////////////////////////////////////////////////////////////////

TChunkReaderSweeper::TChunkReaderSweeper(
    NClusterNode::TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IInvokerPtr storageHeavyInvoker)
    : DynamicConfigManager_(dynamicConfigManager)
    , ChunkReaderSweepExecutor_(New<TPeriodicExecutor>(
        storageHeavyInvoker,
        BIND(&TChunkReaderSweeper::OnChunkReaderSweep, MakeWeak(this)),
        ChunkReaderSweepPeriod))
{
    ChunkReaderSweepExecutor_->Start();
}

void TChunkReaderSweeper::ScheduleChunkReaderSweep(IChunkPtr chunk)
{
    auto dynamicConfig = DynamicConfigManager_->GetConfig();
    ChunkReaderSweepStack_.Enqueue({
        .Chunk = std::move(chunk),
        .Deadline = TInstant::Now() + dynamicConfig->DataNode->ChunkReaderRetentionTimeout
    });
}

void TChunkReaderSweeper::OnChunkReaderSweep()
{
    ChunkReaderSweepStack_.DequeueAll(true, [&] (auto&& entry) {
        ChunkReaderSweepQueue_.push(std::move(entry));
    });

    auto now = TInstant::Now();
    while (!ChunkReaderSweepQueue_.empty() && ChunkReaderSweepQueue_.front().Deadline < now) {
        ChunkReaderSweepQueue_.front().Chunk->TrySweepReader();
        ChunkReaderSweepQueue_.pop();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
