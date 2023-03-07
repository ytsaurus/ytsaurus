#include "journal_manager.h"
#include "journal_node.h"
#include "config.h"
#include "private.h"

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_tree_statistics.h>
#include <yt/server/master/chunk_server/helpers.h>
#include <yt/server/master/chunk_server/chunk_manager.h>

#include <yt/server/master/journal_server/journal_node.h>

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/automaton.h>
#include <yt/server/master/cell_master/multicell_manager.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/ytlib/journal_client/helpers.h>
#include <yt/ytlib/journal_client/journal_ypath_proxy.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NJournalServer {

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
    explicit TImpl(NCellMaster::TBootstrap* bootstrap)
        : NCellMaster::TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::JournalManager)
    { }

    void UpdateStatistics(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics)
    {
        YT_VERIFY(trunkNode->IsTrunk());

        trunkNode->SnapshotStatistics() = *statistics;

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Journal node statistics updated (NodeId: %v, Statistics: %v)",
            trunkNode->GetId(),
            trunkNode->SnapshotStatistics());
    }

    void SealJournal(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics)
    {
        YT_VERIFY(trunkNode->IsTrunk());

        auto* chunkList = trunkNode->GetChunkList();

        trunkNode->SnapshotStatistics() = statistics
            ? *statistics
            :  chunkList->Statistics().ToDataStatistics();

        trunkNode->SetSealed(true);

        if (chunkList && !trunkNode->IsExternal()) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->ScheduleChunkRequisitionUpdate(chunkList);
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Journal node sealed (NodeId: %v)",
            trunkNode->GetId());

        if (trunkNode->IsForeign()) {
            auto req = TJournalYPathProxy::Seal(FromObjectId(trunkNode->GetId()));
            *req->mutable_statistics() = trunkNode->SnapshotStatistics();

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(req, PrimaryMasterCellTag);
        }
    }

    void TruncateJournal(
        TJournalNode* trunkNode,
        i64 rowCount)
    {
        YT_VERIFY(trunkNode->IsTrunk());
        if (!trunkNode->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (trunkNode->IsExternal()) {
            auto req = TJournalYPathProxy::Truncate(FromObjectId(trunkNode->GetId()));
            req->set_row_count(rowCount);
            multicellManager->PostToMaster(req, trunkNode->GetExternalCellTag());
        } else {
            DoTruncateJournal(trunkNode, rowCount);
        }

        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Journal node truncated (NodeId: %v, RowCount: %v)",
            trunkNode->GetId(),
            rowCount);

        if (trunkNode->IsForeign()) {
            auto req = TJournalYPathProxy::UpdateStatistics(FromObjectId(trunkNode->GetId()));
            *req->mutable_statistics() = trunkNode->SnapshotStatistics();
            multicellManager->PostToMaster(req, trunkNode->GetNativeCellTag());
        }
    }

private:
    void DoTruncateJournal(
        TJournalNode* trunkNode,
        i64 rowCount)
    {
        auto* chunkList = trunkNode->GetChunkList();
        YT_VERIFY(chunkList);

        auto currentRowCount = chunkList->Statistics().RowCount;
        if (currentRowCount < rowCount) {
            YT_LOG_DEBUG_UNLESS(IsRecovery(), "Journal has less rows than requested for truncation (CurrentRowCount: %v, RequestedRowCount: %v)",
                currentRowCount,
                rowCount);
            return;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto* newChunkList = chunkManager->CreateChunkList(chunkList->GetKind());
        newChunkList->AddOwningNode(trunkNode);
        objectManager->RefObject(newChunkList);

        auto desiredRowCount = rowCount;

        for (auto* child : chunkList->Children()) {
            if (desiredRowCount <= 0) {
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Dropping chunk when truncating journal (NodeId: %v, ChunkId: %v)",
                    trunkNode->GetId(),
                    child->GetId());
                continue;
            }

            auto* chunk = child->AsChunk();
            const auto& miscExt = chunk->MiscExt();
            YT_VERIFY(miscExt.has_row_count());
            auto childRowCount = miscExt.row_count();
            if (childRowCount <= desiredRowCount) {
                desiredRowCount -= childRowCount;
            } else {
                child->AsChunk()->MiscExt().set_row_count(desiredRowCount);
                YT_LOG_DEBUG_UNLESS(IsRecovery(), "Truncating trailing journal chunk (NodeId: %v, ChunkId: %v, PrevRowCount: %v, NewRowCount: %v)",
                    trunkNode->GetId(),
                    child->GetId(),
                    childRowCount,
                    desiredRowCount);
                desiredRowCount = 0;
            }
            chunkManager->AttachToChunkList(newChunkList, child);
        }

        chunkList->RemoveOwningNode(trunkNode);
        objectManager->UnrefObject(chunkList);

        trunkNode->SetChunkList(newChunkList);
        trunkNode->SnapshotStatistics() = newChunkList->Statistics().ToDataStatistics();
    }
};

////////////////////////////////////////////////////////////////////////////////

TJournalManager::TJournalManager(NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TJournalManager::~TJournalManager() = default;

void TJournalManager::UpdateStatistics(
    TJournalNode* trunkNode,
    const TDataStatistics* statistics)
{
    Impl_->UpdateStatistics(trunkNode, statistics);
}

void TJournalManager::SealJournal(
    TJournalNode* trunkNode,
    const TDataStatistics* statistics)
{
    Impl_->SealJournal(trunkNode, statistics);
}

void TJournalManager::TruncateJournal(
        TJournalNode* trunkNode,
        i64 rowCount)
{
    Impl_->TruncateJournal(trunkNode, rowCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
