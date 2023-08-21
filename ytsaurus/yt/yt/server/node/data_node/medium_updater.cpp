#include "medium_updater.h"

#include "bootstrap.h"
#include "chunk_store.h"
#include "config.h"
#include "location.h"
#include "master_connector.h"
#include "medium_directory_manager.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT::NDataNode {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TMediumUpdater::TMediumUpdater(
    IBootstrap* bootstrap,
    TMediumDirectoryManagerPtr mediumDirectoryManager)
    : Bootstrap_(bootstrap)
    , MediumDirectoryManager_(std::move(mediumDirectoryManager))
{ }

void TMediumUpdater::UpdateLocationMedia(
    const NDataNodeTrackerClient::NProto::TMediumOverrides& protoMediumOverrides,
    bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    THashMap<NChunkClient::TChunkLocationUuid, int> mediumOverrides;
    mediumOverrides.reserve(protoMediumOverrides.overrides_size());

    for (const auto& mediumOverride : protoMediumOverrides.overrides()) {
        auto locationUuid = FromProto<TChunkLocationUuid>(mediumOverride.location_uuid());
        EmplaceOrCrash(mediumOverrides, locationUuid, mediumOverride.medium_index());
    }

    auto mediumDirectory = GetMediumDirectoryOrCrash(onInitialize);

    const auto& chunkStore = Bootstrap_->GetChunkStore();
    for (const auto& location : chunkStore->Locations()) {
        const TMediumDescriptor* descriptor = nullptr;

        if (auto it = mediumOverrides.find(location->GetUuid()); it != mediumOverrides.end()) {
            descriptor = mediumDirectory->FindByIndex(it->second);
            if (!descriptor) {
                YT_LOG_ALERT("Overridden location medium does not exists (LocationId: %v, LocationUuid: %v, MediumIndex: %v)",
                    location->GetId(),
                    location->GetUuid(),
                    it->second);
            }
        }

        if (!descriptor) {
            const auto& mediumName = location->GetStaticConfig()->MediumName;
            descriptor = mediumDirectory->FindByName(mediumName);
            if (!descriptor) {
                YT_LOG_ERROR("Configured location medium does not exist (LocationId: %v, LocationUuid: %v, MediumName: %v)",
                    location->GetId(),
                    location->GetUuid(),
                    mediumName);
                continue;
            }
        }

        location->UpdateMediumDescriptor(*descriptor, onInitialize);
    }
}

TMediumDirectoryPtr TMediumUpdater::GetMediumDirectoryOrCrash(bool onInitialize)
{
    try {
        return MediumDirectoryManager_->GetMediumDirectory();
    } catch (const std::exception& ex) {
        if (onInitialize) {
            throw;
        }

        YT_LOG_FATAL(ex, "Cannot get medium directory after initialization");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

