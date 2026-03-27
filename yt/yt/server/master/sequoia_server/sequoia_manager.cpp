#include "sequoia_manager.h"

#include "config.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/config.h>
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

using NObjectServer::VerifyPersistentStateRead;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransactionManager
    : public ISequoiaManager
    , public TMasterAutomatonPart
{
public:
    explicit TSequoiaTransactionManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::SequoiaTransactionService)
    {
        RegisterMethod(BIND_NO_PROPAGATE(&TSequoiaTransactionManager::HydraStartTransaction, Unretained(this)));
    }

    void StartTransaction(NSequoiaClient::NProto::TReqStartTransaction* request) override
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        if (hydraManager->IsEnteringReadOnlyMode()) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Cannot start Sequoia transaction while entering read-only mode");
        }

        // There is a common problem: if user got OK response on his request
        // there is no any guarantees that 2PC transaction was actually
        // committed. To observe all succeeded (from user point of view) txs
        // we have to wait until all currenly prepared txs are finished.
        // It should be moved to TransactionSupervisor::Prepare when Sequoia tx
        // sequencer will be implemented.
        // TODO(aleksandra-zh): do it.
        if (!request->suppress_strongly_ordered_transaction_barrier()) {
            const auto& transactionSupervisor = Bootstrap_->GetTransactionSupervisor();
            WaitForFast(transactionSupervisor->WaitUntilPreparedTransactionsFinished())
                .ThrowOnError();
        }

        auto transactionId = FromProto<TTransactionId>(request->id());
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());
        ValidateWritePrerequisites(
            transactionId,
            FromProto(request->attributes())->Find<std::string>("title"),
            prerequisiteTransactionIds);

        auto mutation = CreateMutation(hydraManager, *request);
        mutation->SetCurrentTraceContext();

        const auto& config = Bootstrap_->GetDynamicConfig()->SequoiaManager;

        // Optimization: just enqueue mutation without waiting for it. Sequoia
        // transaction prepare will be enqueued later than start so it will be
        // executed later and observe Sequoia transaction start.
        //
        // Without optimization:
        // | client                          | master                          |
        // | send StartSequoiaTransaction    |                                 |
        // |                                 | enqueue StartSequoiaTransaction |
        // |                                 | execute StartSequoiaTransaction |
        // | receive StartSequoiaTransaction |                                 |
        // | send CommitTransaction          |                                 |
        // |                                 | enqueue CommitTransaction       |
        // |                                 | execute CommitTransaction       |
        //
        // With optimization:
        // | client                          | master                          |
        // | send StartSequoiaTransaction    |                                 |
        // |                                 | enqueue StartSequoiaTransaction |
        // | receive StartSequoiaTransaction | execute StartSequoiaTransaction |
        // | send CommitTransaction          |                                 | <- Send next reques without waiting for mutation
        // |                                 | enqueue CommitTransaction       |
        // |                                 | execute CommitTransaction       |
        if (config->EnableAsyncSequoiaTransactionStart) {
            auto failureProbability = config->Testing->SequoiaTransactionStartFailureProbability;
            if (Y_UNLIKELY(failureProbability && RandomNumber<double>() < *failureProbability)) {
                YT_LOG_DEBUG("Sequoia transaction start failed for testing purposes (TransactionId: %v)",
                    transactionId);
            } else {
                mutation
                    ->Commit()
                    .Subscribe(BIND([transactionId] (TErrorOr<TMutationResponse> rsp) {
                        if (!rsp.IsOK()) {
                            YT_LOG_ERROR(TError(rsp), "Failed to start Sequoia transaction (TransactionId: %v)",
                                transactionId);
                        }
                    }));
            }
        } else {
            WaitFor(mutation->Commit())
                .ThrowOnError();
        }
    }

private:
    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    void ValidateWritePrerequisites(
        TTransactionId sequoiaTransactionId,
        std::optional<std::string> sequoiaTransactionTitle,
        const std::vector<TTransactionId>& prerequisiteTransactionIds) const
    {
        YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

        // Fast path.
        if (prerequisiteTransactionIds.empty()) {
            return;
        }

        if (!Bootstrap_->GetConfigManager()->GetConfig()->TransactionManager->EnableCypressMirroredToSequoiaPrerequisiteTransactionValidationViaLeases) {
            YT_LOG_ALERT(
                "Sequoia transaction requires Cypress transaction prerequisites "
                "but prerequisite validation via leases is disabled "
                "(SequoiaTransactionId: %v, SequoiaTransactionTitle: %v, PrerequisiteTransactionIds: %v)",
                sequoiaTransactionId,
                sequoiaTransactionTitle,
                prerequisiteTransactionIds);
            THROW_ERROR_EXCEPTION("Prerequisite transaction validation via leases is disabled");
            return;
        }

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
        const auto& config = Bootstrap_->GetConfigManager()->GetConfig();

        // To set actual user before creating transaction object.
        auto identity = ParseAuthenticationIdentityFromProto(request->identity());
        TAuthenticatedUserGuard userGuard(Bootstrap_->GetSecurityManager(), identity);

        auto transactionId = FromProto<TTransactionId>(request->id());
        auto timeout = FromProto<TDuration>(request->timeout());
        auto prerequisiteTransactionIds = FromProto<std::vector<TTransactionId>>(request->prerequisite_transaction_ids());

        auto attributes = FromProto(request->attributes());
        std::string title;
        if (auto specifiedTitle = attributes->FindAndRemove<std::string>("title")) {
            // NB: it's better to have weird title like "Sequoia transaction:
            // sequoia transaction for something" than to not have any Sequoia
            // mentioning at all.
            if (!specifiedTitle->starts_with("Sequoia transaction")) {
                *specifiedTitle = "Sequoia transaction: " + *specifiedTitle;
            }
            title = std::move(*specifiedTitle);
        } else {
            title = "Sequoia transaction";
        }

        auto enableLeaseIssuing = config->TransactionManager->EnableCypressMirroredToSequoiaPrerequisiteTransactionValidationViaLeases;
        YT_LOG_DEBUG(
            "Starting Sequoia transaction "
            "(TransactionId: %v, Timeout: %v, Title: %v, LeasesToIssue: %v)",
            transactionId,
            timeout,
            title,
            enableLeaseIssuing ? prerequisiteTransactionIds : std::vector<TTransactionId>{});

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        if (transactionManager->FindTransaction(transactionId)) {
            THROW_ERROR_EXCEPTION("Transaction %v already exists", transactionId);
        }

        NTransactionServer::TTransaction* transaction = nullptr;

        try {
            transaction = transactionManager->StartSequoiaTransaction(
                transactionId,
                timeout,
                title,
                *attributes);
        } catch (const std::exception& ex) {
            YT_LOG_ALERT(ex, "Failed to start Sequoia transaction (TransactionId: %v)", transactionId);
            throw;
        }

        transaction->SetSequoiaTransaction(true);
        transaction->SequoiaWriteSet().CopyFrom(request->write_set());

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            auto& action = transaction->Actions().emplace_back(std::move(data));

            YT_LOG_DEBUG("Transaction action registered (TransactionId: %v, ActionType: %v)",
                transactionId,
                action.Type);
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
