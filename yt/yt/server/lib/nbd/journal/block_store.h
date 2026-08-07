#pragma once

#include "private.h"

#include <yt/yt/server/lib/nbd/helpers.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/logging/public.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/range.h>

#include <library/cpp/yt/memory/ref.h>

#include <library/cpp/containers/absl/flat_hash_map.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! A reference to a stored block by its journal-hunk location: which chunk, and where within it the
//! block's hunk (a THunkPayloadHeader followed by the payload) sits.
struct TStoredBlockRef
{
    NChunkClient::TChunkId ChunkId;
    int RecordIndex = 0;
    //! Byte offset of the hunk (its header) within the record.
    i64 RecordOffset = 0;
    //! Length of the block payload, excluding the hunk header.
    i64 PayloadLength = 0;
};

//! One row of a device snapshot: a used block and the journal-hunk location of its payload.
struct TSnapshotBlock
{
    int Index = 0;
    TStoredBlockRef Ref;
};

//! A point-in-time snapshot of a chunk's state.
struct TChunkInfo
{
    NChunkClient::TChunkId ChunkId;
    int ChunkIndex = 0;
    bool RestoredFromSnapshot = false;
    EChunkSealState SealState = EChunkSealState::None;
    i64 RecordCount = 0;
    i64 DataSize = 0;
    i64 ReferencedBlockCount = 0;
    i64 WrittenBlockCount = 0;
    bool Droppable = false;
};

using TChunkBlockCounts = absl::flat_hash_map<NChunkClient::TChunkId, i64>;

//! An interface for storing and then fetching blocks from an external storage.
/*!
 *  Thread affinity: any
 */
struct IBlockStore
    : public TRefCounted
{
    //! Writes blocks to the store, returning an opaque stored block id for each. Every block must be
    //! exactly the store's configured block size; the blocks are ref-held for the write's duration, so
    //! |blocks| need not outlive the call. Each returned id stays live until #ReleaseBlock frees it.
    virtual TFuture<std::vector<TStoredBlockId>> WriteBlocks(
        TRange<TSharedRef> blocks) = 0;

    //! Releases a stored block id (from #WriteBlocks or #RestoreBlocks) once it is unreferenced,
    //! decrementing its chunk's referenced-block count. Once a chunk has none left the store drops it:
    //! unstaging a store-owned chunk, only forgetting a restored one (its snapshot table owns it).
    virtual void ReleaseBlock(TStoredBlockId blockId) = 0;

    //! Fetches blocks from the store under the given workload category.
    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(
        TRange<TStoredBlockId> blockIds,
        EWorkloadCategory workloadCategory) = 0;

    //! Seals the given journal chunks so they can be referenced by an external table, and stops writing
    //! into them (subsequent writes allocate fresh chunks).
    /*!
     *  Used before a snapshot, on exactly the chunks it references. Sealing itself retries until it
     *  succeeds; the returned future is bounded by the configured snapshot seal timeout, so a stuck seal
     *  fails only the snapshot at hand.
     */
    virtual TFuture<void> SealChunks(TRange<NChunkClient::TChunkId> chunkIds) = 0;

    //! Translates stored block ids into their journal-hunk locations.
    /*!
     *  Returns one ref per input id. The caller seals the referenced chunks (see #SealChunks) before a
     *  table may reference them.
     */
    virtual std::vector<TStoredBlockRef> GetBlockRefs(
        TRange<TStoredBlockId> blockIds) = 0;

    //! Must be called before restoring blocks.
    virtual TFuture<void> BeginRestoreBlocks() = 0;

    //! Registers a batch of a device snapshot's blocks (see ISnapshotWriter) so #ReadBlocks can serve
    //! them. Returns one stored block id per |snapshotBlocks| entry, in order.
    /*!
     *  Called repeatedly to restore a snapshot in batches, then once via #EndRestoreBlocks. Only
     *  before any writes.
     */
    virtual TFuture<std::vector<TStoredBlockId>> RestoreBlocks(std::vector<TSnapshotBlock> snapshotBlocks) = 0;

    //! Completes a batched restore: seeds each restored chunk's recovered block counts and, if the store
    //! was constructed with a chunk list, attaches the referenced journal chunks to it so they stay alive
    //! independently of the snapshot table.
    /*!
     *  |chunkBlockCounts| is keyed by chunk id; chunks missing from it keep a zero written-block count.
     */
    virtual TFuture<void> EndRestoreBlocks(const TChunkBlockCounts& chunkBlockCounts) = 0;

    //! Brackets a snapshot save: while one is in progress the store defers freeing chunks, so a chunk the
    //! snapshot will reference is not freed before its table durably references it. At most one at a time.
    virtual void BeginSnapshot() = 0;
    virtual void EndSnapshot() = 0;

    //! Starts background chunk maintenance (sealing, refilling, dropping). Call once the store holds
    //! its final set of chunks.
    virtual void Start() = 0;

    //! Stops background chunk maintenance; idempotent.
    virtual void Stop() = 0;

    //! Snapshots the state of every live chunk.
    virtual std::vector<TChunkInfo> GetChunkInfos() = 0;

    //! Fired once an I/O operation fails; this wedges the device.
    DECLARE_INTERFACE_SIGNAL(void(const TError& error), Failed);
};

DEFINE_REFCOUNTED_TYPE(IBlockStore)

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateJournalBlockStore(
    TJournalBlockStoreConfigPtr config,
    const TBlockDeviceGeometry& geometry,
    TJournalBlockDeviceOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NObjectClient::TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
