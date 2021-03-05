#include "bootstrap.h"

#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>
#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NClusterNode {

using namespace NApi;
using namespace NCellarAgent;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletCellSnapshot(TBootstrap* bootstrap, const IAsyncZeroCopyInputStreamPtr& reader)
{
    const auto& cellarManager = bootstrap->GetCellarManager();
    const auto& cellar = cellarManager->GetCellar(ECellarType::Tablet);

    auto cellId = TCellId::Create();

    // We create fake tablet slot here populating descriptors with the least amount
    // of data such that configuration succeeds.
    {
        auto options = New<TTabletCellOptions>();
        options->SnapshotAccount = "a";
        options->ChangelogAccount = "a";

        NTabletClient::NProto::TCreateTabletSlotInfo protoInfo;
        ToProto(protoInfo.mutable_cell_id(), cellId);
        protoInfo.set_peer_id(0);
        protoInfo.set_options(ConvertToYsonString(*options).ToString());
        protoInfo.set_tablet_cell_bundle("b");

        cellar->CreateOccupant(protoInfo);
    }

    auto occupant = cellar->GetOccupantOrCrash(cellId);

    {
        TCellDescriptor cellDescriptor;
        cellDescriptor.CellId = TGuid{};
        cellDescriptor.ConfigVersion = 0;
        TCellPeerDescriptor peerDescriptor;
        peerDescriptor.SetVoting(true);
        cellDescriptor.Peers = {peerDescriptor};
        auto prerequisiteTransactionId = MakeId(EObjectType::Transaction, 1, 1, 1);

        NTabletClient::NProto::TConfigureTabletSlotInfo protoInfo;
        ToProto(protoInfo.mutable_cell_descriptor(), cellDescriptor);
        ToProto(protoInfo.mutable_prerequisite_transaction_id(), prerequisiteTransactionId);

        cellar->ConfigureOccupant(occupant, protoInfo);
    }

    BIND([=] {
            const auto& hydraManager = occupant->GetHydraManager();
            hydraManager->ValidateSnapshot(reader);
        })
        .AsyncVia(occupant->GetOccupier()->GetOccupierAutomatonInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
