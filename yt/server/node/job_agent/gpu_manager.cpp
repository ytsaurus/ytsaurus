#include "gpu_manager.h"
#include "private.h"

#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/server/node/data_node/master_connector.h>

#include <yt/server/lib/job_agent/gpu_helpers.h>

#include <yt/server/lib/exec_agent/config.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/subprocess.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/client/object_client/helpers.h>

#include <util/folder/iterator.h>

#include <util/string/strip.h>

namespace NYT::NJobAgent {

using namespace NConcurrency;
using namespace NCellNode;
using namespace NApi;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NChunkClient;
using namespace NYTree;
using namespace NCypressClient;
using namespace NDataNode;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobAgentServerLogger;

static constexpr int MaxChunksPerLocateRequest = 10000;

////////////////////////////////////////////////////////////////////////////////

TGpuSlot::TGpuSlot(int deviceNumber)
    : DeviceNumber_(deviceNumber)
{ }

TString TGpuSlot::GetDeviceName() const
{
    return NJobAgent::GetGpuDeviceName(DeviceNumber_);
}

int TGpuSlot::GetDeviceNumber() const
{
    return DeviceNumber_;
}

////////////////////////////////////////////////////////////////////////////////

TGpuManager::TGpuManager(TBootstrap* bootstrap, TGpuManagerConfigPtr config)
    : Bootstrap_(bootstrap)
    , Config_(std::move(config))
{
    auto descriptors = NJobAgent::ListGpuDevices();
    bool testGpu = Bootstrap_->GetConfig()->ExecAgent->JobController->TestGpu;
    if ((!descriptors.empty() || testGpu) && Config_->DriverLayerDirectoryPath) {
        auto driverVersion = Config_->DriverVersion ? *Config_->DriverVersion : GetGpuDriverVersion();
        DriverLayerPath_ = *Config_->DriverLayerDirectoryPath + "/" + driverVersion;

        YT_LOG_INFO("GPU layer specified (Path: %v, Version: %v)",
            DriverLayerPath_,
            driverVersion);

        FetchDriverLayerExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TGpuManager::FetchDriverLayerInfo, MakeWeak(this)),
            Config_->DriverLayerFetchPeriod,
            EPeriodicExecutorMode::Automatic /*mode*/,
            Config_->DriverLayerFetchPeriod /*splay*/);
        FetchDriverLayerExecutor_->Start();
    } else {
        YT_LOG_INFO("No GPU layer specified");
    }
    if (descriptors.empty()) {
        return;
    }

    auto now = TInstant::Now();
    for (const auto& descriptor : descriptors) {
        GpuDevices_.push_back(descriptor.DeviceName);
        FreeSlots_.emplace_back(descriptor.DeviceNumber);

        TGpuInfo info;
        info.UpdateTime = now;
        YT_VERIFY(HealthyGpuInfoMap_.emplace(descriptor.DeviceNumber, info).second);
    }

    HealthCheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TGpuManager::OnHealthCheck, MakeWeak(this)),
        Config_->HealthCheckPeriod);
    HealthCheckExecutor_->Start();
}

void TGpuManager::OnHealthCheck()
{
    try {
        auto gpuInfos = GetGpuInfos(Config_->HealthCheckTimeout);

        THashSet<int> deviceNumbers;
        for (const auto& info : gpuInfos) {
            deviceNumbers.insert(info.Index);
        }

        YT_LOG_DEBUG("Found healthy GPU devices (DeviceNumbers: %v)",
            deviceNumbers);

        std::vector<TError> newAlerts;

        {
            TGuard<TSpinLock> guard(SpinLock_);

            auto now = TInstant::Now();

            std::vector<int> deviceNumbersToRemove;
            for (const auto& [index, _] : HealthyGpuInfoMap_) {
                if (deviceNumbers.find(index) == deviceNumbers.end()) {
                    deviceNumbersToRemove.push_back(index);
                }
            }

            for (int deviceNumber : deviceNumbersToRemove) {
                HealthyGpuInfoMap_.erase(deviceNumber);
            }

            for (auto& gpuInfo : gpuInfos) {
                gpuInfo.UpdateTime = now;
                HealthyGpuInfoMap_[gpuInfo.Index] = gpuInfo;
            }

            std::vector<TGpuSlot> healthySlots;
            for (auto& slot: FreeSlots_) {
                if (HealthyGpuInfoMap_.find(slot.GetDeviceNumber()) == HealthyGpuInfoMap_.end()) {
                    YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                        slot.GetDeviceName());
                    newAlerts.push_back(TError("Found lost GPU device %v",
                        slot.GetDeviceName()));
                } else {
                    healthySlots.emplace_back(std::move(slot));
                }
            }

            FreeSlots_ = std::move(healthySlots);
        }

        for (const auto& alert: newAlerts) {
            Bootstrap_->GetMasterConnector()->RegisterAlert(alert);
        }

    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get healthy GPU devices");
        Bootstrap_->GetMasterConnector()->RegisterAlert(TError("All GPU devices are disabled")
            << ex);
        HealthCheckExecutor_->Stop();

        TGuard<TSpinLock> guard(SpinLock_);
        Disabled_ = true;
    }
}

void TGpuManager::FetchDriverLayerInfo()
{
    try {
        DoFetchDriverLayerInfo();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to fetch GPU layer");
    }
}

void TGpuManager::DoFetchDriverLayerInfo()
{
    TUserObject userObject(DriverLayerPath_);

    {
        YT_LOG_INFO("Fetching GPU layer basic attributes");

        GetUserObjectBasicAttributes(
            Bootstrap_->GetMasterClient(),
            {&userObject},
            NullTransactionId,
            Logger,
            EPermission::Read,
            TGetUserObjectBasicAttributesOptions{
                .SuppressAccessTracking = true,
                .ChannelKind = EMasterChannelKind::Cache,
            }
        );

        if (userObject.Type != EObjectType::File) {
            THROW_ERROR_EXCEPTION("Invalid type of GPU layer object %v: expected %Qlv, actual %Qlv",
                DriverLayerPath_,
                EObjectType::File,
                userObject.Type)
                << TErrorAttribute("path", DriverLayerPath_)
                << TErrorAttribute("expected", EObjectType::File)
                << TErrorAttribute("actual", userObject.Type);
        }
    }

    {
        YT_LOG_INFO("Requesting GPU layer revision");

        auto channel = Bootstrap_->GetMasterClient()->GetMasterChannelOrThrow(
            EMasterChannelKind::Cache,
            userObject.ExternalCellTag);
        TObjectServiceProxy proxy(channel);

        auto req = TYPathProxy::Get(userObject.GetObjectIdPath() + "/@");
        ToProto(req->mutable_attributes()->mutable_keys(), std::vector<TString>{
            "revision"
        });
        SetSuppressAccessTracking(req, true);

        auto rspOrError = WaitFor(proxy.Execute(req));
        THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error requesting GPU layer revision");
        const auto& rsp = rspOrError.Value();

        auto attributes = ConvertToAttributes(NYson::TYsonString(rsp->value()));
        // TODO(mrkastep): use TRevision
        auto revision = attributes->Get<ui64>("revision", 0);
        if (revision == DriverLayerRevision_) {
            YT_LOG_INFO("GPU layer revision not changed, using cached");
            return;
        }
    }

    YT_LOG_INFO("Fetching GPU layer chunk specs");

    auto channel = Bootstrap_->GetMasterClient()->GetMasterChannelOrThrow(
        EMasterChannelKind::Cache,
        userObject.ExternalCellTag);
    TObjectServiceProxy proxy(channel);

    auto req = TFileYPathProxy::Fetch(userObject.GetObjectIdPath());
    AddCellTagToSyncWith(req, CellTagFromId(userObject.ObjectId));

    ToProto(req->mutable_ranges(), std::vector<TReadRange>{ {} });
    SetSuppressAccessTracking(req, true);
    req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);

    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching chunks for GPU layer %v", DriverLayerPath_);
    const auto& rsp = rspOrError.Value();

    std::vector<NChunkClient::NProto::TChunkSpec> chunkSpecs;
    ProcessFetchResponse(
        Bootstrap_->GetMasterClient(),
        rsp,
        userObject.ExternalCellTag,
        Bootstrap_->GetNodeDirectory(),
        MaxChunksPerLocateRequest,
        std::nullopt,
        Logger,
        &chunkSpecs);

    TArtifactKey layerKey;
    ToProto(layerKey.mutable_chunk_specs(), chunkSpecs);
    layerKey.mutable_data_source()->set_type(static_cast<int>(EDataSourceType::File));
    layerKey.mutable_data_source()->set_path(DriverLayerPath_);

    {
        auto guard = Guard(SpinLock_);
        DriverLayerKey_ = std::move(layerKey);
    }
}

int TGpuManager::GetTotalGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return Disabled_ ? 0 : HealthyGpuInfoMap_.size();
}

int TGpuManager::GetFreeGpuCount() const
{
    auto guard = Guard(SpinLock_);
    return Disabled_ ? 0 : FreeSlots_.size();
}

THashMap<int, TGpuInfo> TGpuManager::GetGpuInfoMap() const
{
    auto guard = Guard(SpinLock_);
    return HealthyGpuInfoMap_;
}

const std::vector<TString>& TGpuManager::ListGpuDevices() const
{
    return GpuDevices_;
}

TGpuManager::TGpuSlotPtr TGpuManager::AcquireGpuSlot()
{
    YT_VERIFY(!FreeSlots_.empty());

    auto deleter = [this, this_ = MakeStrong(this)] (TGpuSlot* slot) {
        YT_LOG_DEBUG("Released GPU slot (DeviceName: %v)",
            slot->GetDeviceName());

        auto guard = Guard(this_->SpinLock_);
        if (HealthyGpuInfoMap_.find(slot->GetDeviceNumber()) == HealthyGpuInfoMap_.end()) {
            guard.Release();
            YT_LOG_WARNING("Found lost GPU device (DeviceName: %v)",
                slot->GetDeviceName());
            Bootstrap_->GetMasterConnector()->RegisterAlert(TError("Found lost GPU device %v",
                slot->GetDeviceName()));
        } else {
            this_->FreeSlots_.emplace_back(std::move(*slot));
        }

        delete slot;
    };

    auto guard = Guard(SpinLock_);
    TGpuSlotPtr slot(new TGpuSlot(std::move(FreeSlots_.back())), deleter);
    FreeSlots_.pop_back();

    YT_LOG_DEBUG("Acquired GPU slot (DeviceName: %v)",
        slot->GetDeviceName());
    return slot;
}

std::vector<TArtifactKey> TGpuManager::GetToppingLayers()
{
    auto guard = Guard(SpinLock_);
    if (DriverLayerKey_) {
        return {
            *DriverLayerKey_
        };
    } else if (DriverLayerPath_) {
        THROW_ERROR_EXCEPTION(NExecAgent::EErrorCode::GpuLayerNotFetched, "GPU layer is not fetched yet");
    } else {
        return {};
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
