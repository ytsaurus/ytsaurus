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
    TMediumDirectoryManagerPtr mediumDirectoryManager,
    TDuration legacyMediumUpdaterPeriod)
    : Bootstrap_(bootstrap)
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
    , UseHeartbeats_(true)
    , MediumDirectoryManager_(std::move(mediumDirectoryManager))
{
    LegacyUpdateLocationMediaExecutor_ = New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TMediumUpdater::OnLegacyUpdateLocationMedia, MakeWeak(this), /*onInitialize*/ false),
        legacyMediumUpdaterPeriod);
}

void TMediumUpdater::EnableLegacyMode(bool legacyModeIsEnabled)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (UseHeartbeats_ != !legacyModeIsEnabled) {
        UseHeartbeats_ = !legacyModeIsEnabled;

        if (legacyModeIsEnabled) {
            YT_LOG_INFO("Switching to legacy medium updater");
            LegacyUpdateLocationMediaExecutor_->Start();
        } else {
            YT_LOG_INFO("Switching to heartbeat medium updater");
            LegacyUpdateLocationMediaExecutor_->Stop();
        }
    }
}

void TMediumUpdater::LegacyInitializeLocationMedia()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(!UseHeartbeats_);

    OnLegacyUpdateLocationMedia(/*onInitialize*/ true);
}

void TMediumUpdater::SetPeriod(TDuration legacyMediumUpdatePeriod)
{
    VERIFY_THREAD_AFFINITY_ANY();

    LegacyUpdateLocationMediaExecutor_->SetPeriod(legacyMediumUpdatePeriod);
}

void TMediumUpdater::UpdateLocationMedia(const NDataNodeTrackerClient::NProto::TMediumOverrides& protoMediumOverrides, bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Bootstrap_->IsDataNode()) {
        return;
    }

    YT_VERIFY(UseHeartbeats_);

    TMediumOverrides mediumOverrides;
    mediumOverrides.reserve(protoMediumOverrides.overrides_size());

    for (const auto& mediumOverride : protoMediumOverrides.overrides()) {
        auto locationId = FromProto<TLocationUuid>(mediumOverride.location_id());
        EmplaceOrCrash(mediumOverrides, locationId, mediumOverride.medium_index());
    }

    DoUpdateMedia(mediumOverrides, onInitialize);
}

void TMediumUpdater::OnLegacyUpdateLocationMedia(bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!Bootstrap_->IsDataNode()) {
        return;
    }

    YT_LOG_DEBUG("Starting media update");

    const auto& client = Bootstrap_->GetMasterClient();

    TString myRpcAddress;
    try {
        myRpcAddress = Bootstrap_->GetDefaultLocalAddressOrThrow();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to update media");

        // NB: If an error has happened during registering at primary master then report failure.
        if (onInitialize) {
            throw;
        }

        return;
    }
    auto mediumConfigPath = Format("//sys/data_nodes/%s/@config/medium_overrides", myRpcAddress);
    YT_LOG_DEBUG("Loading node media config (MediumConfigPath: %v)", mediumConfigPath);

    auto mediumConfigOrError = WaitFor(client->GetNode(mediumConfigPath));

    TLegacyMediumOverrides mediumOverrides;

    if (mediumConfigOrError.IsOK()) {
        try {
            mediumOverrides = ConvertTo<TLegacyMediumOverrides>(mediumConfigOrError.Value());
        } catch (const std::exception& ex) {
            // TODO(s-v-m): Also populate alert to MasterConnector.
            YT_LOG_WARNING(ex, "Cannot parse medium config; skipping reconfiguration (MediumConfigPath: %v)", 
                mediumConfigPath);

            if (!onInitialize) {
                return;
            }
        }
    } else if (mediumConfigOrError.FindMatching(NYTree::EErrorCode::ResolveError)){
        YT_LOG_DEBUG("Medium config does not exist at master; Using empty configuration");
    } else {
        YT_LOG_ERROR("Cannot load medium config from master; Skipping reconfiguration");

        if (!onInitialize) {
            return;
        }
    }

    DoLegacyUpdateMedia(mediumOverrides, onInitialize);
}

static TMediumDirectoryPtr GetMediumDirectoryOrCrash(
    const TMediumDirectoryManagerPtr& mediumDirectoryManager,
    bool onInitialize)
{
    try {
        return mediumDirectoryManager->GetMediumDirectory();
    } catch (const std::exception& ex) {
        if (onInitialize) {
            throw;
        }
        
        YT_LOG_FATAL(ex, "Cannot get medium directory after initialization");
    }
}

void TMediumUpdater::DoUpdateMedia(const TMediumOverrides& mediumOverrides, bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& chunkStore = Bootstrap_->GetChunkStore();
    auto mediumDirectory = GetMediumDirectoryOrCrash(MediumDirectoryManager_, onInitialize);

    for (const auto& location : chunkStore->Locations()) {
        const TMediumDescriptor* descriptor = nullptr;

        if (auto it = mediumOverrides.find(location->GetUuid()); it != mediumOverrides.end()) {
            descriptor = mediumDirectory->FindByIndex(it->second);
            if (!descriptor) {
                YT_LOG_ALERT("Overriden location medium does not exists (MediumIndex: %d, Location: %v)",
                    it->second,
                    location->GetId());
            }
        }

        if (!descriptor) {
            const auto& mediumName = location->GetConfig()->MediumName;
            descriptor = mediumDirectory->FindByName(mediumName);
            if (!descriptor) {
                YT_LOG_ERROR("Configured location medium does not exist (MediumName: %v, Location: %v)",
                    mediumName,
                    location->GetId());
                continue;
            }
        }

        location->UpdateMediumDescriptor(*descriptor, onInitialize);
    }
}

void TMediumUpdater::DoLegacyUpdateMedia(const TLegacyMediumOverrides& mediumOverrides, bool onInitialize)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    auto& chunkStore = Bootstrap_->GetChunkStore();
    auto mediumDirectory = GetMediumDirectoryOrCrash(MediumDirectoryManager_, onInitialize);
    
    for (const auto& location : chunkStore->Locations()) {
        auto it = mediumOverrides.find(location->GetUuid());
        auto newName = it != mediumOverrides.end()
            ? it->second
            : location->GetConfig()->MediumName;
        location->UpdateMediumName(newName, mediumDirectory, onInitialize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

