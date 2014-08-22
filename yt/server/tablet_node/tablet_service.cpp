#include "stdafx.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "store_manager.h"
#include "private.h"

#include <core/compression/helpers.h>

#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <server/hydra/hydra_service.h>

#include <server/hydra/hydra_manager.h>
#include <server/hydra/mutation.h>
#include <server/hydra/rpc_helpers.h>

#include <server/query_agent/helpers.h>

#include <server/cell_node/bootstrap.h>
#include <server/cell_node/config.h>

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
            slot->GetAutomatonInvoker(),
            TServiceId(TTabletServiceProxy::GetServiceName(), slot->GetCellGuid()),
            TabletNodeLogger)
        , Slot_(slot)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Slot_);
        YCHECK(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetInvoker(Slot_->GetAutomatonInvoker(EAutomatonThreadQueue::Read)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Write)
            .SetInvoker(Slot_->GetAutomatonInvoker(EAutomatonThreadQueue::Write)));
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

        // Compute the actual timeout and update request.
        auto transactionManager = Slot_->GetTransactionManager();
        auto actualTimeout = transactionManager->GetActualTimeout(timeout);
        request->set_timeout(actualTimeout.MilliSeconds());

        context->SetRequestInfo("TransactionId: %v, StartTimestamp: %v, Timeout: %v",
            transactionId,
            startTimestamp,
            actualTimeout);

        transactionManager
            ->CreateStartTransactionMutation(*request)
            ->Commit()
             .Subscribe(CreateRpcResponseHandler(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Read)
    {
        ValidateActiveLeader();

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto timestamp = TTimestamp(request->timestamp());
        context->SetRequestInfo("TabletId: %v, Timestamp: %v",
            tabletId,
            timestamp);

        auto tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->GetTabletOrThrow(tabletId);

        auto requestData = NCompression::DecompressWithEnvelope(request->Attachments());

        NQueryAgent::ExecuteRequestWithRetries(
            Bootstrap_->GetConfig()->QueryAgent->MaxQueryRetries,
            Logger,
            [&] () {
                auto responseData = tabletManager->Read(
                    tablet,
                    timestamp,
                    requestData);
                response->Attachments() = NCompression::CompressWithEnvelope(
                    responseData,
                    Bootstrap_->GetConfig()->QueryAgent->LookupResponseCodec);
                context->Reply();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Write)
    {
        ValidateActiveLeader();

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        context->SetRequestInfo("TransactionId: %v, TabletId: %v",
            transactionId,
            tabletId);

        auto transactionManager = Slot_->GetTransactionManager();
        auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

        auto tabletManager = Slot_->GetTabletManager();
        auto* tablet = tabletManager->GetTabletOrThrow(tabletId);

        auto requestData = NCompression::DecompressWithEnvelope(request->Attachments());

        tabletManager->Write(
            tablet,
            transaction,
            requestData);

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
