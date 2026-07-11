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
