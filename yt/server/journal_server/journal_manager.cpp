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

void TJournalManager::SealJournal(
    TJournalNode* trunkNode,
    const TDataStatistics* statistics)
{
    Impl_->SealJournal(trunkNode, statistics);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJournalServer
} // namespace NYT
