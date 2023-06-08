#include "master_connector_helpers.h"

#include "cellar.h"
#include "helpers.h"
#include "occupant.h"
#include "private.h"
#include "public.h"

#include <yt/yt/ytlib/cellar_node_tracker_client/proto/cellar_node_tracker_service.pb.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NCellarAgent {

using namespace NCellarClient;
using namespace NCellarNodeTrackerClient::NProto;
using namespace NObjectClient;
using namespace NHiveClient;

////////////////////////////////////////////////////////////////////////////////

void AddCellarInfoToHeartbeatRequest(
    ECellarType /*cellarType*/,
    const ICellarPtr& cellar,
    bool readOnly,
    TReqCellarHeartbeat* request)
{
    {
        auto availableCellSlotCount = cellar->GetAvailableSlotCount();
        auto usedCellSlotCount = cellar->GetOccupantCount();
        if (readOnly) {
            availableCellSlotCount = 0;
            usedCellSlotCount = 0;
        }
        request->mutable_statistics()->set_available_cell_slots(availableCellSlotCount);
        request->mutable_statistics()->set_used_cell_slots(usedCellSlotCount);
    }

    for (const auto& occupant : cellar->Occupants()) {
        auto* protoSlotInfo = request->add_cell_slots();

        if (occupant) {
            TCellInfo cellInfo {
                .CellId = occupant->GetCellId(),
                .ConfigVersion = occupant->GetConfigVersion()
            };

            ToProto(protoSlotInfo->mutable_cell_info(), cellInfo);
            protoSlotInfo->set_peer_state(ToProto<int>(occupant->GetControlState()));
            protoSlotInfo->set_peer_id(occupant->GetPeerId());
            protoSlotInfo->set_dynamic_config_version(occupant->GetDynamicConfigVersion());

            if (const auto& responseKeeper = occupant->GetResponseKeeper()) {
                protoSlotInfo->set_is_response_keeper_warming_up(responseKeeper->IsWarmingUp());
            }
        } else {
            protoSlotInfo->set_peer_state(ToProto<int>(NHydra::EPeerState::None));
        }
    }
}


void UpdateCellarFromHeartbeatResponse(
    ECellarType cellarType,
    const ICellarPtr& cellar,
    const TRspCellarHeartbeat& response)
{
    auto Logger = CellarAgentLogger.WithTag("CellarType: %v", cellarType);

    if (!cellar) {
        YT_LOG_WARNING("Processing heartbeat for non-existing cellar, ignored",
            cellarType);
        return;
    }

    for (const auto& info : response.slots_to_remove()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YT_VERIFY(GetCellarTypeFromCellId(cellId) == cellarType);

        auto slot = cellar->FindOccupant(cellId);
        if (!slot) {
            YT_LOG_WARNING("Requested to remove a non-existing cell, ignored (CellId: %v)",
                cellId);
            continue;
        }

        YT_LOG_DEBUG("Requested to remove cell (CellId: %v)",
            cellId);

        YT_UNUSED_FUTURE(cellar->RemoveOccupant(slot));
    }

    for (const auto& info : response.slots_to_create()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YT_VERIFY(GetCellarTypeFromCellId(cellId) == cellarType);

        if (cellar->GetAvailableSlotCount() == 0) {
            YT_LOG_WARNING("Requested to start cell when all slots are used, ignored (CellId: %v)",
                cellId);
            continue;
        }

        if (cellar->FindOccupant(cellId)) {
            YT_LOG_WARNING("Requested to start cell when this cell is already being served by the node, ignored (CellId: %v)",
                cellId);
            continue;
        }

        YT_LOG_DEBUG("Requested to start cell (CellId: %v)",
            cellId);

        cellar->CreateOccupant(info);
    }

    for (const auto& info : response.slots_to_configure()) {
        auto descriptor = FromProto<TCellDescriptor>(info.cell_descriptor());
        const auto& cellId = descriptor.CellId;
        YT_VERIFY(GetCellarTypeFromCellId(cellId) == cellarType);

        auto slot = cellar->FindOccupant(cellId);
        if (!slot) {
            YT_LOG_WARNING("Requested to configure a non-existing cell, ignored (CellId: %v)",
                cellId);
            continue;
        }

        if (!slot->CanConfigure()) {
            YT_LOG_WARNING("Cannot configure cell in non-configurable state, ignored (CellId: %v, State: %v)",
                cellId,
                slot->GetControlState());
            continue;
        }

        YT_LOG_DEBUG("Configuruing cell (CellId: %v)",
            cellId);

        cellar->ConfigureOccupant(slot, info);
    }

    for (const auto& info : response.slots_to_update()) {
        auto cellId = FromProto<TCellId>(info.cell_id());
        YT_VERIFY(GetCellarTypeFromCellId(cellId) == cellarType);

        auto slot = cellar->FindOccupant(cellId);
        if (!slot) {
            YT_LOG_WARNING("Requested to update dynamic options for a non-existing cell, ignored (CellId: %v)",
                cellId);
            continue;
        }

        YT_LOG_DEBUG("Updating cell dynamic config (CellId: %v)",
            cellId);

        cellar->UpdateOccupant(slot, info);
    }
}

TError UpdateSolomonTags(
    const ICellarManagerPtr& cellarManager,
    ECellarType cellarType,
    const TString& tagName)
{
    auto cellar = cellarManager->FindCellar(cellarType);
    if (!cellar) {
        return {};
    }

    THashSet<TString> seenTags;
    for (const auto& occupant : cellar->Occupants()) {
        if (!occupant) {
            continue;
        }

        std::optional<TString> tag;

        auto dynamicConfig = occupant->GetDynamicOptions();
        if (dynamicConfig) {
            tag = dynamicConfig->SolomonTag;
        }

        if (!tag) {
            tag = occupant->GetCellBundleName();
        }

        seenTags.insert(*tag);
    }

    if (seenTags.size() == 0) {
        NProfiling::TSolomonRegistry::Get()->SetDynamicTags({});
    } else if (seenTags.size() == 1) {
        NProfiling::TSolomonRegistry::Get()->SetDynamicTags({
            {tagName, *seenTags.begin()}
        });
    } else {
        NProfiling::TSolomonRegistry::Get()->SetDynamicTags({});
        return TError("Conflicting profiling tags")
            << TErrorAttribute("cellar_type", cellarType)
            << TErrorAttribute("tags", seenTags);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellarAgent
