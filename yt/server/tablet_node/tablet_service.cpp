#include "stdafx.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "store_manager.h"
#include "private.h"

#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <server/hydra/hydra_service.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/rpc_helpers.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NRpc;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NHydra;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

class TTabletService
    : public THydraServiceBase
{
public:
    TTabletService(
        TTabletSlot* slot,
        NCellNode::TBootstrap* bootstrap)
        : THydraServiceBase(
            slot->GetHydraManager(),
            slot->GetAutomatonInvoker(EAutomatonThreadQueue::Write),
            TServiceId(TTabletServiceProxy::GetServiceName(), slot->GetCellGuid()),
            TabletNodeLogger.GetCategory())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Slot_);
        YCHECK(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetInvoker(Slot_->GetAutomatonInvoker(EAutomatonThreadQueue::Read)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Write));
    }

private:
    TTabletSlot* Slot_;
    NCellNode::TBootstrap* Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, StartTransaction)
    {
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto startTimestamp = TTimestamp(request->start_timestamp());
        auto timeout = request->has_timeout() ? MakeNullable(TDuration::MilliSeconds(request->timeout())) : Null;

        // Compute actual timeout and update request.
        auto transactionManager = Slot_->GetTransactionManager();
        auto actualTimeout = transactionManager->GetActualTimeout(timeout);
        request->set_timeout(actualTimeout.MilliSeconds());

        context->SetRequestInfo("TransactionId: %s, StartTimestamp: %" PRIu64 ", Timeout: %" PRIu64,
            ~ToString(transactionId),
            startTimestamp,
            actualTimeout.MilliSeconds());

        transactionManager
            ->CreateStartTransactionMutation(*request)
            ->OnSuccess(CreateRpcSuccessHandler(context))
            ->Commit();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Read)
    {
        ValidateActiveLeader();

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto timestamp = TTimestamp(request->timestamp());
        context->SetRequestInfo("TabletId: %s, Timestamp: %" PRIu64,
            ~ToString(tabletId),
            timestamp);

        auto tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->GetTabletOrThrow(tabletId);

        tabletManager->Read(
            tablet,
            timestamp,
            request->encoded_request(),
            response->mutable_encoded_response());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Write)
    {
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        context->SetRequestInfo("TransactionId: %s, TabletId: %s",
            ~ToString(transactionId),
            ~ToString(tabletId));

        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        auto tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->GetTabletOrThrow(tabletId);

        tabletManager->Write(
            tablet,
            transaction,
            request->encoded_request());

        context->Reply();
    }

};

IServicePtr CreateTabletService(TTabletSlot* slot, NCellNode::TBootstrap* bootstrap)
{
    return New<TTabletService>(slot, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
