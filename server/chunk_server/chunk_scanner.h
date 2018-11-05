#pragma once

#include "public.h"

#include <yt/server/object_server/public.h>

#include <yt/core/profiling/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NChunkServer {

////////////////////////////////////////////////////////////////////////////////

//! A helper for background chunk scan.
/*!
 *  1. Maintains a queue of chunks to be scanned later. Supports dequeuing
 *  chunks enqueued up to a certain deadline instant.
 *
 *  2. Handles "global chunk scan" on startup.
 *  For #EChunkScanKind::Refresh and #EChunkScanKind::RequisitionUpdate all
 *  chunks are scanned, for #EChunkScanKind::Seal only journal chunks are scanned.
 *  To this aim, all chunks are included into double-linked lists
 *  (cf. #TChunkDynamicData::AllLinkedListNode and #TChunkDynamicData::JournalLinkedListNode).
 *  Scheduling the scan only takes O(1).
 *  New chunks are added to the front of the lists.
 *  Dead chunks are extracted from anywhere.
 *  The scanner walks along the corresponding list in forward direction.
 *
 *  3. Provides the effective size of the queue, including manually queued chunks and
 *  those scheduled for the global scan.
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
        NObjectServer::TObjectManagerPtr objectManager,
        EChunkScanKind kind);

    //! Must be called exactly once upon initialization.
    //! Schedules #chunkCount chunks starting from #frontChunk for the global scan.
    void Start(TChunk* frontChunk, int chunkCont);

    //! Notifies the scanner that a certain #chunk is dead.
    //! Enables advancing global iterator to avoid pointing to dead chunks.
    void OnChunkDestroyed(TChunk* chunk);

    //! Enqueues a given #chunk.
    /*!
     *  If the chunk is already queued (as indicated by its scan flag), does
     *  nothing and returns |false|.
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
     *  In both cases, the returned chunk may be a zombie. It is caller's responsibility
     *  to handle this situation.
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
    const NObjectServer::TObjectManagerPtr ObjectManager_;
    const EChunkScanKind Kind_;

    TChunk* GlobalIterator_ = nullptr;
    int GlobalCount_ = -1;

    struct TQueueEntry
    {
        TChunk* Chunk;
        NProfiling::TCpuInstant Instant;
    };

    std::queue<TQueueEntry> Queue_;

    NLogging::TLogger Logger;

    void AdvanceGlobalIterator();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
