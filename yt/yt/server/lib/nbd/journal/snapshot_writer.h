#pragma once

#include "private.h"
#include "block_store.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NNbd::NJournal {

////////////////////////////////////////////////////////////////////////////////

//! Writes a journal device snapshot into the (already created and resolved) table |userObject|, under
//! its own transaction (userObject.TransactionId).
/*!
 *  Each row's |payload| is a global hunk reference into the |hunkChunkIds|, which are attached
 *  zero-copy to the table's hunk chunk list. The caller commits the transaction.
 *
 *  Blocking; run it on a background invoker.
 */
void WriteJournalSnapshot(
    const NApi::NNative::IClientPtr& client,
    const NChunkClient::TUserObject& userObject,
    TRange<TSnapshotBlock> blocks,
    TRange<NChunkClient::TChunkId> hunkChunkIds,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NJournal
