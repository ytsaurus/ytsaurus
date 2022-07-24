#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A helper for background chunk scan.
/*!
 *  1. Maintains a queue of chunks to be scanned later. Supports dequeuing
 *  chunks enqueued up to a certain deadline instant.
 *
 *  2. Handles "global chunk scan" on startup.
 *  Blob and journal chunks are always scanned separately (because for some
 *  scans, such as #EChunkScanKind::Seal, blob chunks are irrelevant, and for
 *  other scans, such as #EChunkScanKind::Refresh, journal and blob chunks
 *  should be scanned with different priorities).
 *  To this aim, all chunks are included into two global disjoint double-linked
 *  lists - one for blob and one for journal chunks
 *  (cf. #TChunkDynamicData::LinkedListNode)
 *  Scheduling the scan only takes O(1).
 *  New chunks are added to the front of the lists.
 *  Dead chunks are extracted from anywhere.
 *  The scanner walks along the corresponding list in forward direction.
 *
 *  3. Provides the effective size of the queue, including manually queued chunks and
 *  those scheduled for the global scan.
 *
 *  4. Allows to scan chunks which are divided into several shards, including
 *  starting and stopping particular shard scan in O(1) time.
 *
 *  To avoid adding a chunk to the queue multiple times, scan flags are used
 *  (cf. #TChunk::GetScanFlag).
 *
 *  The chunks present in the queue carry an additional ephemeral ref.
 */
class TChunkScanner
{
public:
    TChunkScanner(
        NObjectServer::IObjectManagerPtr objectManager,
        EChunkScanKind kind,
        bool journal);

    //! Starts scan of one of the shards.
    //! Schedules #chunkCount chunks starting from #frontChunk for the global shard scan.
    void Start(int shardIndex, TGlobalChunkScanDescriptor descriptor);

    //! Schedules #chunkCount chunks starting from #frontChunk for the global shard scan.
    void ScheduleGlobalScan(int shardIndex, TGlobalChunkScanDescriptor descriptor);

    //! Stops scan of the shard. No more chunks from the shard will be returned
    //! from the |DequeueChunk| call.
    void Stop(int shardIndex);

    //! Notifies the scanner that a certain #chunk is dead.
    //! Enables advancing global iterator to avoid pointing to dead chunks.
    void OnChunkDestroyed(TChunk* chunk);

    //! Enqueues a given #chunk.
    /*!
     *  If the chunk is already queued (as indicated by its scan flag) or scanner is stopped,
     *  does nothing and returns |false|.
     *
     *  If the chunk belongs to a shard which is not scanned, does nothing and returns |false|.
     *
     *  Otherwise, sets the scan flag, ephemeral-refs the chunk, and enqueues it.
     */
    bool EnqueueChunk(TChunk* chunk);

    //! Tries to dequeue the next chunk.
    /*!
     *  If the global scan is not finished yet, returns the next chunk in the global list.
     *
     *  Otherwise checks the queue and dequeues the next chunk. Ephemeral-unrefs it and clears the
     *  scan flag.
     *
     *  If no chunk is available, returns |nullptr|.
     *
     *  Note that |nullptr| as a result does not mean that there are no more chunks to scan.
     *  Subsequent calls of |DequeueChunk| may return non-null chunks. Existence of the
     *  unscanned chunks should be checked via |HasUnscannedChunk| call.
     *
     */
    TChunk* DequeueChunk();

    //! Returns |true| if there are some unscanned chunks, either scheduled for the global scan
    //! or added manually at #deadline or earlier.
    bool HasUnscannedChunk(NProfiling::TCpuInstant deadline =
        std::numeric_limits<NProfiling::TCpuInstant>::max()) const;

    //! Returns the effective queue size, including both chunks scheduled for the global scan
    //! and added manually.
    int GetQueueSize() const;

private:
    const NObjectServer::IObjectManagerPtr ObjectManager_;
    const EChunkScanKind Kind_;
    const bool Journal_;
    const NLogging::TLogger Logger;

    struct TGlobalChunkScanShard
    {
        TChunk* Iterator = nullptr;
        int ChunkCount = 0;
    };
    std::array<TGlobalChunkScanShard, ChunkShardCount> GlobalChunkScanShards_;
    int ActiveGlobalChunkScanIndex_ = -1;

    std::bitset<ChunkShardCount> ActiveShardIndices_;

    struct TQueueEntry
    {
        NObjectServer::TEphemeralObjectPtr<TChunk> Chunk;
        NProfiling::TCpuInstant Instant;
    };
    std::queue<TQueueEntry> Queue_;

    void AdvanceGlobalIterator(int shardIndex);

    void RecomputeActiveGlobalChunkScanIndex();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
