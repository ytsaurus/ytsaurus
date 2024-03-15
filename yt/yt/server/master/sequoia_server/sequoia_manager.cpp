#include "sequoia_manager.h"

#include "private.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>

#include <yt/yt/server/master/transaction_server/transaction.h>
#include <yt/yt/server/master/transaction_server/transaction_manager.h>

#include <yt/yt/ytlib/transaction_client/action.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NRpc;
using namespace NSecurityServer;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SequoiaServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaTransactionManager
    : public ISequoiaManager
    , public TMasterAutomatonPart
{
public:
    explicit TSequoiaTransactionManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Default)
    {
        RegisterMethod(BIND(&TSequoiaTransactionManager::HydraStartTransaction, Unretained(this)));
    }

    virtual void StartTransaction(NSequoiaClient::NProto::TReqStartTransaction* request)
    {
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        WaitFor(CreateMutation(hydraManager, *request)->Commit())
            .ThrowOnError();
    }

private:
    void HydraStartTransaction(NSequoiaClient::NProto::TReqStartTransaction* request)
    {
        // To set actual user before creating transaction object.
        auto identity = ParseAuthenticationIdentityFromProto(request->identity());
        TAuthenticatedUserGuard userGuard(Bootstrap_->GetSecurityManager(), identity);

        auto transactionId = FromProto<TGuid>(request->id());
        auto timeout = FromProto<TDuration>(request->timeout());

        TString title = "Sequoia transaction";

        auto attributes = FromProto(request->attributes());
        if (auto maybeTitle = attributes->FindAndRemove<TString>("title"); maybeTitle) {
            title = *maybeTitle;
        }

        YT_LOG_DEBUG("Staring Sequoia transaction "
            "(TransactionId: %v, Timeout: %v, Title: %v)",
            transactionId,
            timeout,
            title);

        const auto& transactionManager = Bootstrap_->GetTransactionManager();
        if (transactionManager->FindTransaction(transactionId)) {
            THROW_ERROR_EXCEPTION("Transaction %v already exists", transactionId);
        }

        auto* transaction = transactionManager->StartTransaction(
            /*parent*/ nullptr,
            /*prerequisiteTransactions*/ {},
            /*replicatedToCellTags*/ {},
            timeout,
            /*deadline*/ std::nullopt,
            title,
            *attributes,
            /*isSequoiaTransaction*/ false,
            transactionId);
        transaction->SetIsSequoiaTransaction(true);
        transaction->SequoiaWriteSet().CopyFrom(request->write_set());

        for (const auto& protoData : request->actions()) {
            auto data = FromProto<TTransactionActionData>(protoData);
            transaction->Actions().push_back(data);
        }

        transaction->SetAuthenticationIdentity(std::move(identity));
    }
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaManagerPtr CreateSequoiaManager(TBootstrap* bootstrap)
{
    return New<TSequoiaTransactionManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
