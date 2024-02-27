#include "journal_manager.h"
#include "journal_node.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_tree_statistics.h>
#include <yt/yt/server/master/chunk_server/helpers.h>
#include <yt/yt/server/master/chunk_server/chunk_manager.h>

#include <yt/yt/server/master/journal_server/journal_node.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/multicell_manager.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/journal_client/helpers.h>
#include <yt/yt/ytlib/journal_client/journal_ypath_proxy.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NJournalServer {

using namespace NCellMaster;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NHydra;
using namespace NObjectServer;
using namespace NJournalClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JournalServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TJournalManager
    : public IJournalManager
    , public TMasterAutomatonPart
{
public:
    explicit TJournalManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::JournalManager)
    { }

    void UpdateStatistics(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics) override
    {
        YT_VERIFY(trunkNode->IsTrunk());

        trunkNode->SnapshotStatistics() = *statistics;

        YT_LOG_DEBUG("Journal node statistics updated (NodeId: %v, Statistics: %v)",
            trunkNode->GetId(),
            trunkNode->SnapshotStatistics());
    }

    void SealJournal(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics) override
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

        YT_LOG_DEBUG("Journal node sealed (NodeId: %v)",
            trunkNode->GetId());

        if (trunkNode->IsForeign()) {
            auto req = TJournalYPathProxy::Seal(FromObjectId(trunkNode->GetId()));
            *req->mutable_statistics() = trunkNode->SnapshotStatistics();

            const auto& multicellManager = Bootstrap_->GetMulticellManager();
            multicellManager->PostToMaster(req, trunkNode->GetNativeCellTag());
        }
    }

    void TruncateJournal(
        TJournalNode* trunkNode,
        i64 desiredRowCount) override
    {
        YT_VERIFY(trunkNode->IsTrunk());
        if (!trunkNode->GetSealed()) {
            THROW_ERROR_EXCEPTION("Journal is not sealed");
        }

        if (desiredRowCount < 0) {
            THROW_ERROR_EXCEPTION(
                "Truncation desired row count %v is negative",
                desiredRowCount);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();

        if (trunkNode->IsExternal()) {
            auto req = TJournalYPathProxy::Truncate(FromObjectId(trunkNode->GetId()));
            req->set_row_count(desiredRowCount);
            multicellManager->PostToMaster(req, trunkNode->GetExternalCellTag());
        } else {
            DoTruncateJournal(trunkNode, desiredRowCount);
        }

        YT_LOG_DEBUG("Journal node truncated (NodeId: %v, RowCount: %v)",
            trunkNode->GetId(),
            desiredRowCount);

        if (trunkNode->IsForeign()) {
            auto req = TJournalYPathProxy::UpdateStatistics(FromObjectId(trunkNode->GetId()));
            *req->mutable_statistics() = trunkNode->SnapshotStatistics();
            multicellManager->PostToMaster(req, trunkNode->GetNativeCellTag());
        }
    }

private:
    void DoTruncateJournal(
        TJournalNode* trunkNode,
        i64 desiredRowCount)
    {
        auto* chunkList = trunkNode->GetChunkList();
        YT_VERIFY(chunkList);

        auto totalRowCount = chunkList->Statistics().RowCount;
        if (totalRowCount < desiredRowCount) {
            YT_LOG_DEBUG(
                "Journal has less rows than requested for truncation (TotalRowCount: %v, DesiredRowCount: %v)",
                totalRowCount,
                desiredRowCount);
            return;
        }

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto* newChunkList = chunkManager->CreateChunkList(chunkList->GetKind());
        newChunkList->AddOwningNode(trunkNode);

        i64 appendedRowCount = 0;
        for (auto* child : chunkList->Children()) {
            YT_VERIFY(appendedRowCount <= desiredRowCount);
            if (appendedRowCount == desiredRowCount) {
                YT_LOG_DEBUG(
                    "Dropping chunk when truncating journal (NodeId: %v, ChunkId: %v)",
                    trunkNode->GetId(),
                    child->GetId());
                continue;
            }

            auto* chunk = child->AsChunk();
            auto firstOverlayedRowIndex = chunk->GetFirstOverlayedRowIndex();
            auto chunkRowCount = chunk->GetRowCount();
            auto newRowCount = GetJournalRowCount(appendedRowCount, firstOverlayedRowIndex, chunkRowCount);
            if (newRowCount <= appendedRowCount) {
                YT_LOG_DEBUG(
                    "Dropping nested chunk (NodeId: %v, ChunkId: %v, AppendedRowCount: %v, ChunkRowCount: %v)",
                    trunkNode->GetId(),
                    child->GetId(),
                    appendedRowCount,
                    chunkRowCount);
                continue;
            }

            if (newRowCount > desiredRowCount) {
                auto rowCountToTrim = newRowCount - desiredRowCount;
                YT_LOG_DEBUG(
                    "Truncating trailing journal chunk (NodeId: %v, ChunkId: %v, PrevRowCount: %v, NewRowCount: %v)",
                    trunkNode->GetId(),
                    child->GetId(),
                    chunkRowCount,
                    chunkRowCount - rowCountToTrim);
                chunkRowCount -= rowCountToTrim;
                newRowCount = desiredRowCount;
            }

            // Temporarily set row count to logical row count
            // for AttachToChunkList to compute statistics correctly
            // (it is not the sum of row counts for overlayed chunks).
            chunk->SetRowCount(newRowCount - appendedRowCount);
            chunkManager->AttachToChunkList(newChunkList, {child});
            // Then set it back.
            chunk->SetRowCount(chunkRowCount);

            YT_VERIFY(newChunkList->Statistics().RowCount == newRowCount);
            appendedRowCount = newRowCount;
        }

        chunkList->RemoveOwningNode(trunkNode);

        trunkNode->SetChunkList(newChunkList);

        YT_VERIFY(newChunkList->Statistics().RowCount == desiredRowCount);
        trunkNode->SnapshotStatistics() = newChunkList->Statistics().ToDataStatistics();
    }
};

////////////////////////////////////////////////////////////////////////////////

IJournalManagerPtr CreateJournalManager(TBootstrap* bootstrap)
{
    return New<TJournalManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
