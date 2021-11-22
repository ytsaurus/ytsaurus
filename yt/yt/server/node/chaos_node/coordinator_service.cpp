#include "coordinator_service.h"

#include "private.h"
#include "bootstrap.h"
#include "chaos_slot.h"
#include "coordinator_manager.h"
#include "transaction_manager.h"

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>

#include <yt/yt/ytlib/chaos_client/coordinator_service_proxy.h>

#include <yt/yt/ytlib/chaos_client/proto/coordinator_service.pb.h>

namespace NYT::NChaosNode {

using namespace NRpc;
using namespace NChaosClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TCoordinatorService
    : public THydraServiceBase
{
public:
    explicit TCoordinatorService(IChaosSlotPtr slot)
        : THydraServiceBase(
            slot->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Default),
            TCoordinatorServiceProxy::GetDescriptor(),
            ChaosNodeLogger,
            slot->GetCellId())
        , Slot_(std::move(slot))
    {
        YT_VERIFY(Slot_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(SuspendCoordinator));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ResumeCoordinator));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegisterTransactionActions));
    }

private:
    const IChaosSlotPtr Slot_;

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, SuspendCoordinator)
    {
        context->SetRequestInfo();

        const auto& coordinatorManager = Slot_->GetCoordinatorManager();
        coordinatorManager->SuspendCoordinator(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, ResumeCoordinator)
    {
        context->SetRequestInfo();

        const auto& coordinatorManager = Slot_->GetCoordinatorManager();
        coordinatorManager->ResumeCoordinator(std::move(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NChaosClient::NProto, RegisterTransactionActions)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto transactionStartTimestamp = request->transaction_start_timestamp();
        auto transactionTimeout = FromProto<TDuration>(request->transaction_timeout());

        context->SetRequestInfo("TransactionId: %v, TransactionStartTimestamp: %llx, TransactionTimeout: %v, ActionCount: %v",
            transactionId,
            transactionStartTimestamp,
            transactionTimeout,
            request->actions_size());

        const auto& transactionManager = Slot_->GetTransactionManager();
        transactionManager
            ->CreateRegisterTransactionActionsMutation(context)
            ->CommitAndReply(context);
    }


    IHydraManagerPtr GetHydraManager() override
    {
        return Slot_->GetHydraManager();
    }
};

IServicePtr CreateCoordinatorService(IChaosSlotPtr slot)
{
    return New<TCoordinatorService>(std::move(slot));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
