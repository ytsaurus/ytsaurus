#include "sequoia_manager.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/lease_server/lease_manager.h>
#include <yt/yt/server/lib/lease_server/helpers.h>

#include <yt/yt/server/lib/transaction_supervisor/transaction_supervisor.h>

#include <yt/yt/ytlib/transaction_client/action.h>
#include <yt/yt/ytlib/transaction_client/transaction_service_proxy.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NLeaseServer;
using namespace NObjectClient;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransactionManager
    : public ISequoiaManager
    , public TMasterAutomatonPart
{
public:
    explicit TSequoiaTransactionManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Default)
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TSequoiaTransactionManager::HydraStartTransaction, Unretained(this)));
    }

    virtual void StartTransaction(NSequoiaClient::NProto::TReqStartTransaction* request)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // There is a common problem: if user got OK response on his request
        // there is no any guarantees that 2PC transaction was actually
        // committed. To observe all succeeded (from user point of view) txs
        // we have to wait until all currenly prepared txs are finished.
        // It should be moved to TransactionSupervisor::Prepare when Sequoia tx
        // sequencer will be implemented.
        // TODO(aleksandra-zh): do it.
        const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
        WaitFor(transactionSupervisor->WaitUntilPreparedTransactionsFinished())
            .ThrowOnError();

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        auto mutation = CreateMutation(hydraManager, *request);
        mutation->SetCurrentTraceContext();

        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        ValidateWritePrerequisites(prerequisiteTransactionIds);

        WaitFor(mutation->Commit())
            .ThrowOnError();
    }

private:
    void ValidateWritePrerequisites(const std::vector<TTransactionId>& prerequisiteTransactionIds)
    {
        // Fast path.
        if (prerequisiteTransactionIds.empty()) {
            return;
        }

        const auto& leaseManager = Bootstrap_->GetLeaseManager();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        const auto& hiveManager = Bootstrap_->GetHiveManager();

        auto resultOrError = WaitFor(IssueLeasesForCell(
            prerequisiteTransactionIds,
            leaseManager,
            hiveManager,
            multicellManager->GetCellId(),
            /*synWithAllLeaseTransactionCoordinators*/ true,
            BIND([multicellManager] (TCellTag cellTag) {
                return multicellManager->GetCellId(cellTag);
            }),
            BIND([multicellManager] (TCellTag cellTag) {
                return multicellManager->FindMasterChannel(cellTag, EPeerKind::Leader);
            })));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            resultOrError,
            NObjectClient::EErrorCode::PrerequisiteCheckFailed,
            "Failed to issue leases for prerequisite transactions");
    }

    void HydraStartTransaction(NSequoiaClient::NProto::TReqStartTransaction* request)
    {
        // To set actual user before creating transaction object.
        auto identity = ParseAuthenticationIdentityFromProto(request->identity());
        TAuthenticatedUserGuard userGuard(Bootstrap_->GetSecurityManager(), identity);

        auto transactionId = FromProto<TGuid>(request->id());
        auto timeout = FromProto<TDuration>(request->timeout());
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());

        auto attributes = FromProto(request->attributes());
        auto title = attributes->FindAndRemove<std::string>("title").value_or("Sequoia transaction");

        YT_LOG_DEBUG("Staring Sequoia transaction "
            "(TransactionId: %v, Timeout: %v, Title: %v, IssuedLeaseIds: %v)",
            transactionId,
            timeout,
            title,
            prerequisiteTransactionIds);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        if (transactionManager->FindTransaction(transactionId)) {
            THROW_ERROR_EXCEPTION("Transaction %v already exists", transactionId);
        }

        // TODO(cherepashka): add user-friendly handling of errors above.
        auto* transaction = transactionManager->StartSystemTransaction(
            /*replicatedToCellTags*/ {},
            timeout,
            title,
            *attributes,
            transactionId);

        transaction->SetSequoiaTransaction(true);
        transaction->SequoiaWriteSet().CopyFrom(request->write_set());

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);
        }

        transaction->SetAuthenticationIdentity(std::move(identity));
        transaction->SetTraceContext(TryGetCurrentTraceContext());
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaManagerPtr CreateSequoiaManager(TBootstrap* bootstrap)
{
    return New<TSequoiaTransactionManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
