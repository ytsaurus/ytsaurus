#include "stdafx.h"
#include "tablet_service.h"
#include "tablet_slot.h"
#include "tablet_manager.h"
#include "transaction_manager.h"
#include "transaction.h"
#include "private.h"

#include <ytlib/tablet_client/tablet_service_proxy.h>

#include <ytlib/chunk_client/memory_reader.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/reader.h>
#include <ytlib/new_table_client/chunk_reader.h>

#include <server/hydra/hydra_manager.h>

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
}

DEFINE_RPC_SERVICE_METHOD(TTabletService, Write)
{
    auto transactionId = FromProto<TTransactionId>(request->transaction_id());
    auto tabletId = FromProto<TTabletId>(request->tablet_id());
    context->SetRequestInfo("TransactionId: %s, TabletId: %s",
        ~ToString(transactionId),
        ~ToString(tabletId));

    auto transactionManager = Slot->GetTransactionManager();
    auto* transaction = transactionManager->GetTransactionOrThrow(transactionId);

    auto memoryReader = New<TMemoryReader>(
        std::move(context->RequestAttachments()),
        std::move(*request->mutable_chunk_meta()));
    auto chunkReader = CreateChunkReader(
        New<TChunkReaderConfig>(),
        memoryReader);

    auto tabletManager = Slot->GetTabletManager();
    auto* tablet = tabletManager->GetTabletOrThrow(tabletId);
    tabletManager->Write(tablet, transaction, std::move(chunkReader));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
