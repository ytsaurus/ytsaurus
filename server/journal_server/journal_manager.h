#pragma once

#include "public.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/cell_master/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NJournalServer {

////////////////////////////////////////////////////////////////////////////////

class TJournalManager
    : public TRefCounted
{
public:
    TJournalManager(
        TJournalManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TJournalManager();

    //! Marks the journal as sealed and updates its snapshot statistics.
    //! If #statistics is |nullptr| then computes one from chunk lists.
    //! For secondary masters, this call also notifies the primary.
    void SealJournal(
        NJournalServer::TJournalNode* trunkNode,
        const NChunkClient::NProto::TDataStatistics* statistics);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
