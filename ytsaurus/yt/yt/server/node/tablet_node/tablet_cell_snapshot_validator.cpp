#include "bootstrap.h"

#include <yt/yt/server/node/cellar_node/bootstrap.h>

#include <yt/yt/server/lib/cellar_agent/cellar_manager.h>
#include <yt/yt/server/lib/cellar_agent/cellar.h>
#include <yt/yt/server/lib/cellar_agent/occupant.h>

#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/tablet_client/config.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NCellarClient;
using namespace NCellarAgent;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NCellarNodeTrackerClient::NProto;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto FakeTransactionId = MakeWellKnownId(EObjectType::Transaction, TCellTag(1));
static const TString FakeAccount = "fake-account";
static const TString FakeBundle = "fake-bundle";

////////////////////////////////////////////////////////////////////////////////

ICellarOccupantPtr CreateFakeOccupant(IBootstrapBase* bootstrap, TGuid cellId)
{
    const auto& cellarManager = bootstrap
        ->GetCellarNodeBootstrap()
        ->GetCellarManager();
    const auto& cellar = cellarManager->GetCellar(ECellarType::Tablet);

    // We create fake tablet slot here populating descriptors with the least amount
    // of data such that configuration succeeds.
    {
        auto options = New<TTabletCellOptions>();
        options->SnapshotAccount = FakeAccount;
        options->ChangelogAccount = FakeAccount;

        TCreateCellSlotInfo protoInfo;
        ToProto(protoInfo.mutable_cell_id(), cellId);
        protoInfo.set_peer_id(0);
        protoInfo.set_options(ConvertToYsonString(*options).ToString());
        protoInfo.set_cell_bundle(FakeBundle);

        cellar->CreateOccupant(protoInfo);
    }

    auto occupant = cellar->GetOccupant(cellId);

    {
        TCellDescriptor cellDescriptor;
        cellDescriptor.CellId = cellId;
        cellDescriptor.ConfigVersion = 0;
        TCellPeerDescriptor peerDescriptor;
        peerDescriptor.SetVoting(true);
        cellDescriptor.Peers = {peerDescriptor};

        TConfigureCellSlotInfo protoInfo;
        ToProto(protoInfo.mutable_cell_descriptor(), cellDescriptor);
        ToProto(protoInfo.mutable_prerequisite_transaction_id(), FakeTransactionId);

        cellar->ConfigureOccupant(occupant, protoInfo);
    }

    return occupant;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
