#include "stdafx.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "private.h"

#include <core/rpc/server.h>

#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <server/hydra/hydra_manager.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NTabletNode {

using namespace NChunkClient;
using namespace NTabletClient;
using namespace NTableClient;
using namespace NVersionedTableClient;
using namespace NCellNode;

////////////////////////////////////////////////////////////////////////////////

TTabletService::TTabletService(
    TTabletSlot* slot,
    TBootstrap* bootstrap)
    : NHydra::THydraServiceBase(
        slot->GetHydraManager(),
        slot->GetAutomatonInvoker(),
        NRpc::TServiceId(TTabletServiceProxy::GetServiceName(), slot->GetCellGuid()),
        TabletNodeLogger.GetCategory())
    , Slot(slot)
    , Bootstrap(bootstrap)
{
    YCHECK(Slot);
    YCHECK(Bootstrap);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(Write));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Lookup));
}

void TTabletService::Start()
{
    auto rpcServer = Bootstrap->GetRpcServer();
    rpcServer->RegisterService(this);
}

void TTabletService::Stop()
{
    auto rpcServer = Bootstrap->GetRpcServer();
    rpcServer->UnregisterService(this);
}

DEFINE_RPC_SERVICE_METHOD(TTabletService, Write)
{
    ValidateActiveLeader();

    auto transactionId = FromProto<TTransactionId>(request->transaction_id());
    auto tabletId = FromProto<TTabletId>(request->tablet_id());
    context->SetRequestInfo("TransactionId: %s, TabletId: %s",
        ~ToString(transactionId),
        ~ToString(tabletId));

    auto transactionManager = Slot->GetTransactionManager();
    auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

    auto tabletManager = Slot->GetTabletManager();
    auto* tablet = tabletManager->GetTabletOrThrow(tabletId);
    tabletManager->Write(
        tablet,
        transaction,
        std::move(*request->mutable_chunk_meta()),
        std::move(context->RequestAttachments()));

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTabletService, Lookup)
{
    ValidateActiveLeader();

    auto tabletId = FromProto<TTabletId>(request->tablet_id());
    auto timestamp = TTimestamp(request->timestamp());
    auto key = FromProto<TOwningRow>(request->key());
    
    context->SetRequestInfo("TabletId: %s, Timestamp: %" PRId64,
        ~ToString(tabletId),
        timestamp);

    auto tabletManager = Slot->GetTabletManager();
    auto* tablet = tabletManager->GetTabletOrThrow(tabletId);
    tabletManager->Lookup(
        tablet,
        key,
        timestamp,
        response->mutable_chunk_meta(),
        &response->Attachments());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
