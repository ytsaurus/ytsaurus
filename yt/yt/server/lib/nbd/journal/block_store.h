#pragma once

#include "private.h"

#include <yt/yt/server/lib/nbd/helpers.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/logging/public.h>

#include <library/cpp/yt/memory/range.h>

#include <library/cpp/yt/memory/ref.h>

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

//! An interface for storing and then fetching blocks from an external storage.
/*!
 *  Thread affinity: any
 */
struct IBlockStore
    : public TRefCounted
{
    //! Puts blocks to the store. Returns opaque block ids, one per input block.
    /*!
     *  Every block must be exactly of the store's configured block size. The blocks are ref-held
     *  for the duration of the write, so |blocks| itself need not outlive the call.
     */
    virtual TFuture<std::vector<TStoredBlockId>> WriteBlocks(
        TRange<TSharedRef> blocks) = 0;

    //! Fetches blocks from the store.
    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(
        TRange<TStoredBlockId> blockIds,
        const NChunkClient::TClientChunkReadOptions& options) = 0;

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
     *  The chunks holding them must have been sealed (see #SealChunks). Returns one ref per input id.
     */
    virtual std::vector<TStoredBlockRef> GetBlockRefs(
        TRange<TStoredBlockId> blockIds) = 0;

    //! Registers pre-existing snapshot blocks (see #WriteJournalSnapshot) so #ReadBlocks can serve them.
    /*!
     *  If the store was constructed with a chunk list, the referenced journal chunks are attached to
     *  it so they stay alive independently of the snapshot table. Must be called once, before any
     *  writes. Returns one stored block id per input ref.
     */
    virtual TFuture<std::vector<TStoredBlockId>> RestoreBlocks(
        TRange<TStoredBlockRef> blockRefs) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockStore)

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateJournalBlockStore(
    TJournalBlockStoreConfigPtr config,
    TBlockDeviceGeometry geometry,
    TJournalBlockDeviceOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NObjectClient::TTransactionId transactionId,
    NChunkClient::TChunkListId chunkListId,
    IInvokerPtr invoker,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
