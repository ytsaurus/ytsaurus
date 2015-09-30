#include "stdafx.h"
#include "journal_manager.h"
#include "journal_node.h"
#include "config.h"
#include "private.h"

#include <server/chunk_server/chunk.h>
#include <server/chunk_server/chunk_tree_statistics.h>
#include <server/chunk_server/helpers.h>
#include <server/chunk_server/chunk_manager.h>

#include <server/journal_server/journal_node.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/automaton.h>
#include <server/cell_master/multicell_manager.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

#include <ytlib/journal_client/helpers.h>
#include <ytlib/journal_client/journal_ypath_proxy.h>

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NJournalServer {

using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectServer;
using namespace NJournalClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JournalServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TJournalManager::TImpl
    : public NCellMaster::TMasterAutomatonPart
{
public:
    TImpl(
        TJournalManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : NCellMaster::TMasterAutomatonPart(bootstrap)
        , Config_(config)
    { }


    void SealChunk(TChunk* chunk, const TMiscExt& info)
    {
        if (!chunk->IsJournal()) {
            THROW_ERROR_EXCEPTION("Not a journal chunk");
        }

        if (!chunk->IsConfirmed()) {
            THROW_ERROR_EXCEPTION("Chunk is not confirmed");
        }

        if (chunk->IsSealed()) {
            THROW_ERROR_EXCEPTION("Chunk is already sealed");
        }

        chunk->Seal(info);
        OnChunkSealed(chunk);

        if (IsLeader()) {
            auto chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->ScheduleChunkRefresh(chunk);
        }

        LOG_DEBUG_UNLESS(IsRecovery(), "Chunk sealed (ChunkId: %v, RowCount: %v, UncompressedDataSize: %v, CompressedDataSize: %v)",
            chunk->GetId(),
            info.row_count(),
            info.uncompressed_data_size(),
            info.compressed_data_size());
    }

    void SealJournal(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics)
    {
        YCHECK(trunkNode->IsTrunk());

        trunkNode->SnapshotStatistics() = statistics
            ? *statistics
            :  trunkNode->GetChunkList()->Statistics().ToDataStatistics();

        trunkNode->SetSealed(true);

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->UpdateAccountNodeUsage(trunkNode);

        LOG_DEBUG_UNLESS(IsRecovery(), "Journal node sealed (NodeId: %v)",
            trunkNode->GetId());

        auto objectManager = Bootstrap_->GetObjectManager();
        if (objectManager->IsForeign(trunkNode)) {
            auto req = TJournalYPathProxy::Seal(FromObjectId(trunkNode->GetId()));
            *req->mutable_statistics() = trunkNode->SnapshotStatistics();

            auto multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(req, PrimaryMasterCellTag);
        }
    }


    void OnChunkSealed(TChunk* chunk)
    {
        YASSERT(chunk->IsSealed());

        if (chunk->Parents().empty())
            return;

        // Go upwards and apply delta.
        YCHECK(chunk->Parents().size() == 1);
        auto* chunkList = chunk->Parents()[0];

        auto statisticsDelta = chunk->GetStatistics();
        AccumulateUniqueAncestorsStatistics(chunkList, statisticsDelta);

        auto securityManager = Bootstrap_->GetSecurityManager();

        auto owningNodes = GetOwningNodes(chunk);

        bool journalNodeLocked = false;
        TJournalNode* trunkJournalNode = nullptr;
        for (auto* node : owningNodes) {
            securityManager->UpdateAccountNodeUsage(node);
            if (node->GetType() == EObjectType::Journal) {
                auto* journalNode = static_cast<TJournalNode*>(node);
                if (journalNode->GetUpdateMode() != EUpdateMode::None) {
                    journalNodeLocked = true;
                }
                if (trunkJournalNode) {
                    YCHECK(journalNode->GetTrunkNode() == trunkJournalNode);
                } else {
                    trunkJournalNode = journalNode->GetTrunkNode();
                }    
            }
        }

        if (!journalNodeLocked && IsObjectAlive(trunkJournalNode)) {
            SealJournal(trunkJournalNode, nullptr);
        }
    }

private:
    const TJournalManagerConfigPtr Config_;
};

////////////////////////////////////////////////////////////////////////////////

TJournalManager::TJournalManager(
    TJournalManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TJournalManager::~TJournalManager()
{ }

void TJournalManager::SealChunk(TChunk* chunk, const TMiscExt& info)
{
    Impl_->SealChunk(chunk, info);
}

void TJournalManager::OnChunkSealed(TChunk* chunk)
{
    Impl_->OnChunkSealed(chunk);
}

void TJournalManager::SealJournal(
    TJournalNode* trunkNode,
    const TDataStatistics* statistics)
{
    Impl_->SealJournal(trunkNode, statistics);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT
