#include "stdafx.h"
#include "tablet_service.h"
#include "tablet.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "slot_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "store_manager.h"
#include "security_manager.h"
#include "private.h"

#include <core/compression/helpers.h>

#include <ytlib/tablet_client/tablet_service_proxy.h>
#include <ytlib/tablet_client/wire_protocol.h>

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
using namespace NCompression;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NHydra;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

class TTabletService
    : public THydraServiceBase
{
public:
    TTabletService(
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap)
        : THydraServiceBase(
            slot->GetHydraManager()->CreateGuardedAutomatonInvoker(
                slot->GetAutomatonInvoker()),
            TServiceId(TTabletServiceProxy::GetServiceName(), slot->GetCellId()),
            TabletNodeLogger,
            TTabletServiceProxy::GetProtocolVersion())
        , Slot_(slot)
        , Bootstrap_(bootstrap)
    {
        YCHECK(Slot_);
        YCHECK(Bootstrap_);

        RegisterMethod(RPC_SERVICE_METHOD_DESC(StartTransaction)
            .SetInvoker(Slot_->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Write)));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read)
            .SetCancelable(true)
            .SetInvoker(Bootstrap_->GetQueryPoolInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Write)
            .SetInvoker(Slot_->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::Write)));
    }

private:
    const TTabletSlotPtr Slot_;
    NCellNode::TBootstrap* const Bootstrap_;


    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, StartTransaction)
    {
        ValidatePeer(EPeerKind::Leader);

        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto startTimestamp = TTimestamp(request->start_timestamp());
        auto timeout = TDuration::MilliSeconds(request->timeout());

        auto config = Bootstrap_->GetConfig()->TabletNode->TransactionManager;
        auto actualTimeout = std::min(timeout, config->MaxTransactionTimeout);
        request->set_timeout(actualTimeout.MilliSeconds());

        context->SetRequestInfo("TransactionId: %v, StartTimestamp: %v, Timeout: %v",
            transactionId,
            startTimestamp,
            actualTimeout);

        auto transactionManager = Slot_->GetTransactionManager();
        transactionManager
            ->CreateStartTransactionMutation(*request)
            ->Commit()
             .Subscribe(CreateRpcResponseHandler(context));
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Read)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto timestamp = TTimestamp(request->timestamp());
        auto requestData = DecompressWithEnvelope(request->Attachments());

        context->SetRequestInfo("TabletId: %v, Timestamp: %v",
            tabletId,
            timestamp);

        const auto& user = context->GetUser();
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto config = Bootstrap_->GetConfig()->QueryAgent;

        NQueryAgent::ExecuteRequestWithRetries(
            config->MaxQueryRetries,
            Logger,
            [&] () {
                auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

                TWireProtocolReader reader(requestData);
                TWireProtocolWriter writer;

                auto tabletManager = tabletSnapshot->Slot->GetTabletManager();
                tabletManager->Read(
                    tabletSnapshot,
                    timestamp,
                    &reader,
                    &writer);

                auto responseData = writer.Flush();
                auto responseCodec = request->has_response_codec()
                    ? ECodec(request->response_codec())
                    : ECodec(ECodec::None);
                response->Attachments() = CompressWithEnvelope(responseData,  responseCodec);
                context->Reply();
            });
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Write)
    {
        ValidatePeer(EPeerKind::Leader);

        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());

        context->SetRequestInfo("TabletId: %v, TransactionId: %v",
            tabletId,
            transactionId);

        const auto& user = context->GetUser();
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);

        if (tabletSnapshot->Slot != Slot_) {
            THROW_ERROR_EXCEPTION("Wrong tablet slot: expected %v, got %v",
                Slot_->GetCellId(),
                tabletSnapshot->Slot->GetCellId());
        }

        auto requestData = NCompression::DecompressWithEnvelope(request->Attachments());
        TWireProtocolReader reader(requestData);

        auto tabletManager = Slot_->GetTabletManager();

        while (!reader.IsFinished()) {
            tabletManager->Write(
                tabletSnapshot,
                transactionId,
                &reader);
        }

        context->Reply();
    }


    // THydraServiceBase overrides.
    virtual IHydraManagerPtr GetHydraManager() override
    {
        return Slot_->GetHydraManager();
    }
    
};

IServicePtr CreateTabletService(TTabletSlotPtr slot, NCellNode::TBootstrap* bootstrap)
{
    return New<TTabletService>(slot, bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
