#include "tablet_service.h"
#include "private.h"
#include "security_manager.h"
#include "slot_manager.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"
#include "tablet_slot.h"
#include "transaction.h"
#include "transaction_manager.h"

#include <yt/server/cell_node/bootstrap.h>
#include <yt/server/cell_node/config.h>

#include <yt/server/hydra/hydra_manager.h>
#include <yt/server/hydra/hydra_service.h>
#include <yt/server/hydra/mutation.h>

#include <yt/server/query_agent/helpers.h>

#include <yt/ytlib/tablet_client/tablet_service_proxy.h>
#include <yt/ytlib/tablet_client/wire_protocol.h>

#include <yt/ytlib/table_client/row_buffer.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/core/compression/helpers.h>

namespace NYT {
namespace NTabletNode {

using namespace NYTree;
using namespace NRpc;
using namespace NCompression;
using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NTransactionClient;
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
            slot->GetGuardedAutomatonInvoker(),
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
        auto timeout = FromProto<TDuration>(request->timeout());

        auto config = Bootstrap_->GetConfig()->TabletNode->TransactionManager;
        auto actualTimeout = std::min(timeout, config->MaxTransactionTimeout);
        request->set_timeout(ToProto(actualTimeout));

        context->SetRequestInfo("TransactionId: %v, StartTimestamp: %v, Timeout: %v",
            transactionId,
            startTimestamp,
            actualTimeout);

        auto transactionManager = Slot_->GetTransactionManager();
        transactionManager
            ->CreateStartTransactionMutation(*request)
            ->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NTabletClient::NProto, Read)
    {
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto timestamp = TTimestamp(request->timestamp());
        // TODO(sandello): Extract this out of RPC request.
        auto workloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserRealtime);
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
                slotManager->ValidateTabletAccess(
                    tabletSnapshot,
                    EPermission::Read,
                    timestamp);

                TWireProtocolReader reader(requestData);
                TWireProtocolWriter writer;

                const auto& tabletManager = tabletSnapshot->TabletManager;
                tabletManager->Read(
                    tabletSnapshot,
                    timestamp,
                    workloadDescriptor,
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
        auto tabletId = FromProto<TTabletId>(request->tablet_id());
        auto mountRevision = request->mount_revision();
        auto transactionId = FromProto<TTransactionId>(request->transaction_id());
        auto atomicity = AtomicityFromTransactionId(transactionId);
        auto durability = EDurability(request->durability());

        context->SetRequestInfo("TabletId: %v, TransactionId: %v, Atomicity: %v, Durability: %v",
            tabletId,
            transactionId,
            atomicity,
            durability);

        // NB: Must serve the whole request within a single epoch.
        TCurrentInvokerGuard invokerGuard(Slot_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::Write));

        const auto& user = context->GetUser();
        auto securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, user);

        auto slotManager = Bootstrap_->GetTabletSlotManager();
        auto tabletSnapshot = slotManager->GetTabletSnapshotOrThrow(tabletId);
        slotManager->ValidateTabletAccess(
            tabletSnapshot,
            EPermission::Write,
            SyncLastCommittedTimestamp);

        if (tabletSnapshot->CellId != Slot_->GetCellId()) {
            THROW_ERROR_EXCEPTION("Wrong cell id: expected %v, got %v",
                Slot_->GetCellId(),
                tabletSnapshot->CellId);
        }

        if (tabletSnapshot->Atomicity != atomicity) {
            THROW_ERROR_EXCEPTION("Invalid atomicity mode: %Qlv instead of %Qlv",
                atomicity,
                tabletSnapshot->Atomicity);
        }

        if (tabletSnapshot->Config->ReadOnly) {
            THROW_ERROR_EXCEPTION("Table is read-only");
        }

        tabletSnapshot->ValiateMountRevision(mountRevision);

        auto requestData = NCompression::DecompressWithEnvelope(request->Attachments());
        TWireProtocolReader reader(requestData);

        auto tabletManager = Slot_->GetTabletManager();

        TFuture<void> commitResult;
        while (!reader.IsFinished()) {
            // Due to possible row blocking, serving the request may involve a number of write attempts.
            // Each attempt causes a mutation to be enqueued to Hydra.
            // Since all these mutations are enqueued within a single epoch, only the last commit outcome is
            // actually relevant.
            tabletManager->Write(
                tabletSnapshot,
                transactionId,
                &reader,
                &commitResult);
        }

        switch (durability) {
            case EDurability::Sync:
                context->ReplyFrom(commitResult);
                break;

            case EDurability::Async:
                context->Reply();
                break;

            default:
                YUNREACHABLE();
        }
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
