#pragma once

#include "public.h"

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

struct IJournalManager
    : public virtual TRefCounted
{
    virtual void UpdateStatistics(
        TJournalNode* trunkNode,
        const NChunkClient::NProto::TDataStatistics* statistics) = 0;

    //! Marks the journal as sealed and updates its snapshot statistics.
    //! If #statistics is |nullptr| then computes one from chunk lists.
    //! For secondary masters, this call also notifies the primary.
    virtual void SealJournal(
        TJournalNode* trunkNode,
        const NChunkClient::NProto::TDataStatistics* statistics) = 0;

    virtual void TruncateJournal(
        TJournalNode* trunkNode,
        i64 rowCount) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJournalManager)

////////////////////////////////////////////////////////////////////////////////

IJournalManagerPtr CreateJournalManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
