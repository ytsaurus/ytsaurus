#pragma once

#include "public.h"

#include <server/chunk_server/public.h>

#include <server/cell_master/public.h>

#include <ytlib/chunk_client/public.h>

#include <core/actions/future.h>

namespace NYT {
namespace NJournalServer {

////////////////////////////////////////////////////////////////////////////////

class TJournalManager
    : public TRefCounted
{
public:
    TJournalManager(
        TJournalManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TJournalManager();

    //! Seals the chunk and the journals containing it.
    void SealChunk(
        NChunkServer::TChunk* chunk,
        const NChunkClient::NProto::TMiscExt& info);

    void OnChunkSealed(NChunkServer::TChunk* chunk);

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

} // namespace NJournalServer
} // namespace NYT
