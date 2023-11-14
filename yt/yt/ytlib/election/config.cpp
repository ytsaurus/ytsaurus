#include "config.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NElection {

using namespace NYson;
using namespace NYTree;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

void TCellPeerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("address", &TThis::Address)
        .Default();
    registrar.Parameter("alien_cluster", &TThis::AlienCluster)
        .Default();
    registrar.Parameter("voting", &TThis::Voting)
        .Default(true);
}

TString ToString(const TCellPeerConfigPtr& config)
{
    TStringBuilder builder;
    builder.AppendFormat("%v", config->Address);
    if (config->AlienCluster) {
        builder.AppendFormat("@%v", *config->AlienCluster);
    }
    if (!config->Voting) {
        builder.AppendString(" (non-voting)");
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

void TCellConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_id", &TThis::CellId);
    registrar.Parameter("peers", &TThis::Peers);

    registrar.Postprocessor([] (TThis* config) {
        auto type = TypeFromId(config->CellId);
        if (type != EObjectType::MasterCell && type != EObjectType::TabletCell) {
            THROW_ERROR_EXCEPTION("Cell id %v has invalid type %Qlv",
                config->CellId,
                type);
        }

        auto cellTag = CellTagFromId(config->CellId);
        if (cellTag < MinValidCellTag || cellTag > MaxValidCellTag) {
            THROW_ERROR_EXCEPTION("Cell id %v has invalid cell tag",
                config->CellId);
        }

        int votingPeerCount = 0;
        for (const auto& peer : config->Peers) {
            if (peer->Voting) {
                ++votingPeerCount;
            }
        }

        if (votingPeerCount == 0) {
            THROW_ERROR_EXCEPTION("No voting peers found");
        }
    });
}

void TCellConfig::ValidateAllPeersPresent()
{
    for (int index = 0; index < std::ssize(Peers); ++index) {
        if (!Peers[index]->Address) {
            THROW_ERROR_EXCEPTION("Peer %v is missing in configuration of cell %v",
                index,
                CellId);
        }
    }
}

int TCellConfig::CountVotingPeers() const
{
    int votingPeerCount = 0;
    for (const auto& peer : Peers) {
        if (peer->Voting) {
            ++votingPeerCount;
        }
    }

    return votingPeerCount;
}

int TCellConfig::FindPeerId(const TString& address) const
{
    for (int id = 0; id < std::ssize(Peers); ++id) {
        const auto& peerAddress = Peers[id]->Address;
        if (peerAddress && to_lower(*peerAddress) == to_lower(address)) {
            return id;
        }
    }
    return InvalidPeerId;
}

int TCellConfig::GetPeerIdOrThrow(const TString& address) const
{
    auto id = FindPeerId(address);
    if (id == InvalidPeerId) {
        THROW_ERROR_EXCEPTION("Cannot determine id of peer %Qv", address);
    }
    return id;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

