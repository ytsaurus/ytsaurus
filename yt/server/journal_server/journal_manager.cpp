#include "journal_manager.h"
#include "journal_node.h"
#include "config.h"
#include "private.h"

#include <yt/server/chunk_server/chunk.h>
#include <yt/server/chunk_server/chunk_tree_statistics.h>
#include <yt/server/chunk_server/helpers.h>
#include <yt/server/chunk_server/chunk_manager.h>

#include <yt/server/journal_server/journal_node.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/automaton.h>
#include <yt/server/cell_master/multicell_manager.h>

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
    TImpl(
        TJournalManagerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap)
        : NCellMaster::TMasterAutomatonPart(bootstrap, NCellMaster::EAutomatonThreadQueue::JournalManager)
        , Config_(config)
    { }


    void SealJournal(
        TJournalNode* trunkNode,
        const TDataStatistics* statistics)
    {
        YCHECK(trunkNode->IsTrunk());

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


private:
    const TJournalManagerConfigPtr Config_;

};

////////////////////////////////////////////////////////////////////////////////

TJournalManager::TJournalManager(
    TJournalManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TJournalManager::~TJournalManager() = default;

void TJournalManager::SealJournal(
    TJournalNode* trunkNode,
    const TDataStatistics* statistics)
{
    Impl_->SealJournal(trunkNode, statistics);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalServer
