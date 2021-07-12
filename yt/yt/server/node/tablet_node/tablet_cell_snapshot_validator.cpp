#include "bootstrap.h"

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>
#include <yt/yt/server/lib/cellar_agent/occupier.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/async_stream.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NCellarClient;
using namespace NCellarAgent;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NCellarNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

void ValidateTabletCellSnapshot(IBootstrapBase* bootstrap, const IAsyncZeroCopyInputStreamPtr& reader)
{
    const auto& cellarManager = bootstrap
        ->GetCellarNodeBootstrap()
        ->GetCellarManager();
    const auto& cellar = cellarManager->GetCellar(ECellarType::Tablet);

    auto cellId = MakeRandomId(EObjectType::TabletCell, 1);

    // We create fake tablet slot here populating descriptors with the least amount
    // of data such that configuration succeeds.
    {
        auto options = New<TTabletCellOptions>();
        options->SnapshotAccount = "a";
        options->ChangelogAccount = "a";

        TCreateCellSlotInfo protoInfo;
        ToProto(protoInfo.mutable_cell_id(), cellId);
        protoInfo.set_peer_id(0);
        protoInfo.set_options(ConvertToYsonString(*options).ToString());
        protoInfo.set_cell_bundle("b");

        cellar->CreateOccupant(protoInfo);
    }

    auto occupant = cellar->GetOccupantOrCrash(cellId);

    {
        TCellDescriptor cellDescriptor;
        cellDescriptor.CellId = cellId;
        cellDescriptor.ConfigVersion = 0;
        TCellPeerDescriptor peerDescriptor;
        peerDescriptor.SetVoting(true);
        cellDescriptor.Peers = {peerDescriptor};
        auto prerequisiteTransactionId = MakeId(EObjectType::Transaction, 1, 1, 1);

        TConfigureCellSlotInfo protoInfo;
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

} // namespace NYT::NTabletNode
