#include "job_fs_secretary.h"

#include "bootstrap.h"
#include "gpu_manager.h"
#include "helpers.h"
#include "preparation_options.h"

#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::string MakeNbdDeviceId(TJobId jobId, int nbdDeviceIndex)
{
    auto nbdDeviceId = jobId.Underlying();
    nbdDeviceId.Parts32[0] = nbdDeviceIndex;
    return ToString(nbdDeviceId);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

namespace {

TError CheckArtifacts(
    const std::vector<TArtifactDescription>& baseline,
    const std::vector<TArtifactDescription>& current)
{
    if (baseline.size() != current.size()) {
        return TError("Job spec artifacts count differs from the first job in this allocation")
            << TErrorAttribute("baseline_count", baseline.size())
            << TErrorAttribute("current_count", current.size());
    }
    for (int i = 0; i < ssize(current); ++i) {
        if (!baseline[i].IsStaticDescriptionEqualTo(current[i])) {
            return TError("Job spec artifacts differ from the first job in this allocation")
                << TErrorAttribute("artifact_index", i)
                << TErrorAttribute("baseline", Format("%v", baseline[i].Key))
                << TErrorAttribute("current", Format("%v", current[i].Key));
        }
    }
    return {};
}

TError CheckLayerArtifactKeyList(
    const std::vector<TArtifactKey>& baseline,
    const std::vector<TArtifactKey>& current,
    TStringBuf layerKind)
{
    if (baseline.size() != current.size()) {
        return TError("Job spec %v layer artifact keys count differs from the first job in this allocation", layerKind)
            << TErrorAttribute("baseline_count", baseline.size())
            << TErrorAttribute("current_count", current.size());
    }
    for (int i = 0; i < ssize(current); ++i) {
        if (baseline[i] != current[i]) {
            return TError("Job spec %v layer artifact keys differ from the first job in this allocation", layerKind)
                << TErrorAttribute("layer_index", i)
                << TErrorAttribute("baseline", Format("%v", baseline[i]))
                << TErrorAttribute("current", Format("%v", current[i]));
        }
    }
    return {};
}

TError CheckLayerArtifactKeys(
    const std::vector<TArtifactKey>& baselineRoot,
    const std::vector<TArtifactKey>& currentRoot,
    const std::vector<TArtifactKey>& baselineGpu,
    const std::vector<TArtifactKey>& currentGpu)
{
    if (auto error = CheckLayerArtifactKeyList(baselineRoot, currentRoot, "root volume"); !error.IsOK()) {
        return error;
    }
    if (auto error = CheckLayerArtifactKeyList(baselineGpu, currentGpu, "GPU check volume"); !error.IsOK()) {
        return error;
    }
    return {};
}

TError CheckDockerImage(
    const std::optional<std::string>& baseline,
    const std::optional<std::string>& current)
{
    if (baseline != current) {
        return TError("Job spec docker image differs from the first job in this allocation")
            << TErrorAttribute("baseline", baseline)
            << TErrorAttribute("current", current);
    }
    return {};
}

TError CheckRootVolumeDiskSpaceAndInodeLimit(
    std::optional<i64> baselineDiskSpace,
    std::optional<i64> currentDiskSpace,
    std::optional<i64> baselineInodeLimit,
    std::optional<i64> currentInodeLimit)
{
    if (baselineDiskSpace != currentDiskSpace) {
        return TError("Job spec root volume disk space differs from the first job in this allocation")
            << TErrorAttribute("baseline", baselineDiskSpace)
            << TErrorAttribute("current", currentDiskSpace);
    }
    if (baselineInodeLimit != currentInodeLimit) {
        return TError("Job spec root volume inode limit differs from the first job in this allocation")
            << TErrorAttribute("baseline", baselineInodeLimit)
            << TErrorAttribute("current", currentInodeLimit);
    }
    return {};
}

TError CheckNonRootVolumeParams(
    const std::vector<TBaseVolumeParamsPtr>& baseline,
    const std::vector<TBaseVolumeParamsPtr>& current)
{
    if (baseline.size() != current.size()) {
        return TError("Job spec non-root volume params count differs from the first job in this allocation")
            << TErrorAttribute("baseline_count", baseline.size())
            << TErrorAttribute("current_count", current.size());
    }

    // Sort volumes by VolumeId for comparison since protobuf map iteration order is not guaranteed.
    auto sortByVolumeId = [] (const TBaseVolumeParamsPtr& a, const TBaseVolumeParamsPtr& b) {
        return a->VolumeId < b->VolumeId;
    };

    auto sortedBaseline = baseline;
    auto sortedCurrent = current;
    std::sort(sortedBaseline.begin(), sortedBaseline.end(), sortByVolumeId);
    std::sort(sortedCurrent.begin(), sortedCurrent.end(), sortByVolumeId);

    for (int i = 0; i < ssize(sortedCurrent); ++i) {
        if (!(*sortedBaseline[i] == *sortedCurrent[i])) {
            return TError("Job spec non-root volume params differ from the first job in this allocation")
                << TErrorAttribute("volume_index", i)
                << TErrorAttribute("baseline", Format("%v", sortedBaseline[i]))
                << TErrorAttribute("current", Format("%v", sortedCurrent[i]));
        }
    }
    return {};
}

TError CheckJobVolumeMounts(
    const std::vector<TVolumeMountPtr>& baseline,
    const std::vector<TVolumeMountPtr>& current)
{
    if (baseline.size() != current.size()) {
        return TError("Job spec job volume mounts count differs from the first job in this allocation")
            << TErrorAttribute("baseline_count", baseline.size())
            << TErrorAttribute("current_count", current.size());
    }
    for (int i = 0; i < ssize(current); ++i) {
        if (*baseline[i] != *current[i]) {
            return TError("Job spec job volume mounts differ from the first job in this allocation")
                << TErrorAttribute("mount_index", i)
                << TErrorAttribute("baseline", baseline[i])
                << TErrorAttribute("current", current[i]);
        }
    }
    return {};
}

TError CheckSidecarsVolumeMounts(
    const THashMap<std::string, std::vector<TVolumeMountPtr>>& baseline,
    const THashMap<std::string, std::vector<TVolumeMountPtr>>& current)
{
    if (baseline.size() != current.size()) {
        return TError("Job spec sidecar volume mounts count differs from the first job in this allocation")
            << TErrorAttribute("baseline_count", baseline.size())
            << TErrorAttribute("current_count", current.size());
    }

    for (const auto& [baselineSidecarName, baselineSidecarMounts] : baseline) {
        auto it = current.find(baselineSidecarName);
        if (it == current.end()) {
            return TError("Job spec sidecar volume mounts count differs from the first job in this allocation")
                << TErrorAttribute("baseline_count", baseline.size())
                << TErrorAttribute("current_count", current.size());
        }
        const auto& currentSidecarMounts = it->second;
        for (int i = 0; i < ssize(current); ++i) {
            if (*baselineSidecarMounts[i] != *currentSidecarMounts[i]) {
                return TError("Job spec sidecar volume mounts differ from the first job in this allocation")
                    << TErrorAttribute("mount_index", i)
                    << TErrorAttribute("baseline", baselineSidecarMounts[i])
                    << TErrorAttribute("current", currentSidecarMounts[i]);
            }
        }
    }
    return {};
}

TError CheckSandboxNbdRootVolumeData(
    const std::optional<TSandboxNbdRootVolumeData>& baseline,
    const std::optional<TSandboxNbdRootVolumeData>& current)
{
    if (baseline.has_value() != current.has_value()) {
        return TError("Job spec sandbox NBD root volume data presence differs from the first job in this allocation")
            << TErrorAttribute("baseline_has_value", baseline.has_value())
            << TErrorAttribute("current_has_value", current.has_value());
    }
    if (!baseline) {
        return {};
    }
    if (*baseline != *current) {
        return TError("Job spec sandbox NBD root volume data differs from the first job in this allocation")
            << TErrorAttribute("baseline", Format("%v", *baseline))
            << TErrorAttribute("current", Format("%v", *current));
    }
    return {};
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJobFSSecretary::TJobFSSecretary(
    IBootstrap* bootstrap,
    NLogging::TLogger logger)
    : Bootstrap_(bootstrap)
    , BaseLogger_(std::move(logger))
    , Logger(BaseLogger_)
{ }

void TJobFSSecretary::ConfigureFromSpec(
    TJobId jobId,
    const TJobSpecExt& jobSpecExt,
    const TUserJobSpec* userJobSpec,
    int userId,
    bool hasNbdServer,
    bool enableVirtualSandbox,
    bool enableRootVolumeDiskQuota,
    bool needGpuLayers)
{
    const bool firstConfiguration = !JobId_;

    OnNewJobStarted(jobId);
    RootVolumeDiskQuotaEnabled_ = enableRootVolumeDiskQuota;

    {
        // Build description directly into a local struct so the result
        // does not depend on any prior member-variable state.
        auto current = New<TJobFSDescription>();
        ConfigureUserArtifacts(current, userJobSpec);
        ConfigureLayerArtifacts(current, userJobSpec);
        ConfigureDockerImage(current, userJobSpec);
        ConfigureUdfArtifacts(current, jobSpecExt);
        ConfigureVolumes(current, userJobSpec, userId);

        if (enableVirtualSandbox && hasNbdServer) {
            MarkArtifactsAccessedViaVirtualSandbox(current, userJobSpec);
        }
        MarkArtifactsAccessedViaBind(current);

        ConfigureNbdDeviceIds(current);

        // Baseline verification. The whole description (including GPU topping
        // layers and access flags) is now built before this point.
        // For the first job Description_ is empty, so we just apply it.
        // For subsequent jobs Description_ still holds the first job's data
        // (OnNewJobStarted does not clear it), so we compare against it.
        if (!firstConfiguration) {
            VerifyDescriptionMatchesApplied(current);
        } else {
            ApplyDescription(std::move(current));
        }

        ActualDockerImage_ = Description_->DockerImage;

        MergedGpuCheckVolumeLayerArtifactKeys_ = Description_->GpuCheckVolumeLayerArtifactKeys;
        MergedRootVolumeLayerArtifactKeys_ = Description_->RootVolumeLayerArtifactKeys;

        if (needGpuLayers && userJobSpec) {
            AddGpuToppingLayersIfNeeded(userJobSpec);
        }
    }

    CheckConfiguration(hasNbdServer);
}

void TJobFSSecretary::VerifyDescriptionMatchesApplied(const TJobFSDescriptionPtr& current) const
{
    auto crashIfFailed = [&] (const TError& error) {
        YT_LOG_FATAL_UNLESS(
            error.IsOK(),
            error,
            "Job spec differs from the first job in this allocation (CurrentJobId: %v)",
            JobId_);
    };

    crashIfFailed(CheckArtifacts(Description_->Artifacts, current->Artifacts));
    crashIfFailed(CheckLayerArtifactKeys(
        Description_->RootVolumeLayerArtifactKeys,
        current->RootVolumeLayerArtifactKeys,
        Description_->GpuCheckVolumeLayerArtifactKeys,
        current->GpuCheckVolumeLayerArtifactKeys));
    crashIfFailed(CheckDockerImage(Description_->DockerImage, current->DockerImage));

    crashIfFailed(CheckRootVolumeDiskSpaceAndInodeLimit(
            Description_->RootVolumeDiskSpace,
            current->RootVolumeDiskSpace,
            Description_->RootVolumeInodeLimit,
            current->RootVolumeInodeLimit));
    crashIfFailed(CheckNonRootVolumeParams(Description_->NonRootVolumeParams, current->NonRootVolumeParams));
    crashIfFailed(CheckJobVolumeMounts(Description_->JobVolumeMounts, current->JobVolumeMounts));
    crashIfFailed(CheckSidecarsVolumeMounts(Description_->SidecarsVolumeMounts, current->SidecarsVolumeMounts));
    crashIfFailed(CheckSandboxNbdRootVolumeData(Description_->SandboxNbdRootVolumeData, current->SandboxNbdRootVolumeData));
}

void TJobFSSecretary::ApplyDescription(TJobFSDescriptionPtr&& description)
{
    Description_ = std::move(description);
}

void TJobFSSecretary::CheckConfiguration(bool hasNbdServer) const
{
    if (Description_->SandboxNbdRootVolumeData && !hasNbdServer) {
        THROW_ERROR_EXCEPTION(
            NExecNode::EErrorCode::NbdServerDisabledOnNode,
            "NBD server disabled on this node but job requested nbd volume");
    }
}

void TJobFSSecretary::ConfigureUserArtifacts(TNonNullPtr<TJobFSDescription> description, const TUserJobSpec* userJobSpec)
{
    if (!userJobSpec) {
        return;
    }

    for (const auto& descriptor : userJobSpec->files()) {
        description->Artifacts.push_back(TArtifactDescription{
            .SandboxKind = ESandboxKind::User,
            .Name = descriptor.file_name(),
            .Executable = descriptor.executable(),
            .BypassArtifactCache = descriptor.bypass_artifact_cache(),
            .CopyFile = descriptor.copy_file(),
            .Key = TArtifactKey(descriptor),
        });
        EmplaceOrCrash(description->UserArtifactNameToIndex, descriptor.file_name(), description->Artifacts.size() - 1);
    }
}

void TJobFSSecretary::AddGpuToppingLayersIfNeeded(const TUserJobSpec* userJobSpec)
{
    if (!userJobSpec->enable_gpu_layers()) {
        return;
    }

    if (Description_->RootVolumeLayerArtifactKeys.empty()) {
        THROW_ERROR_EXCEPTION(
            NExecNode::EErrorCode::GpuJobWithoutLayers,
            "No layers specified for GPU job; at least a base layer is required to use GPU");
    }

    auto toppingLayers = Bootstrap_->GetGpuManager()->GetToppingLayers();

    if (!userJobSpec->gpu_check_volume_layers().empty()) {
        MergedGpuCheckVolumeLayerArtifactKeys_.insert(
            MergedGpuCheckVolumeLayerArtifactKeys_.begin(),
            toppingLayers.begin(),
            toppingLayers.end());
    }

    MergedRootVolumeLayerArtifactKeys_.insert(
            MergedRootVolumeLayerArtifactKeys_.begin(),
            std::make_move_iterator(toppingLayers.begin()),
            std::make_move_iterator(toppingLayers.end()));
}

void TJobFSSecretary::ConfigureLayerArtifacts(TNonNullPtr<TJobFSDescription> description, const TUserJobSpec* userJobSpec)
{
    if (!userJobSpec) {
        return;
    }

    for (const auto& layerKey : userJobSpec->gpu_check_volume_layers()) {
        description->GpuCheckVolumeLayerArtifactKeys.emplace_back(layerKey);
    }
}

void TJobFSSecretary::ConfigureDockerImage(TNonNullPtr<TJobFSDescription> description, const TUserJobSpec* userJobSpec)
{
    if (userJobSpec && userJobSpec->has_docker_image()) {
        description->DockerImage = userJobSpec->docker_image();
    }
}

void TJobFSSecretary::ConfigureUdfArtifacts(TNonNullPtr<TJobFSDescription> description, const TJobSpecExt& jobSpecExt)
{
    if (!jobSpecExt.has_input_query_spec()) {
        return;
    }

    const auto& querySpec = jobSpecExt.input_query_spec();
    for (const auto& function : querySpec.external_functions()) {
        TArtifactKey key;
        key.mutable_data_source()->set_type(ToProto(EDataSourceType::File));

        for (const auto& chunkSpec : function.chunk_specs()) {
            *key.add_chunk_specs() = chunkSpec;
        }

        description->Artifacts.push_back(TArtifactDescription{
            .SandboxKind = ESandboxKind::Udf,
            .Name = function.name(),
            .Executable = false,
            .BypassArtifactCache = false,
            .CopyFile = false,
            .Key = key,
        });
    }
}

void TJobFSSecretary::ConfigureNbdDeviceIds(TNonNullPtr<TJobFSDescription> description)
{
    // Mark NBD layers with NBD device ids.
    int nbdDeviceCount = 0;
    for (auto& layer : description->RootVolumeLayerArtifactKeys) {
        if (NYT::FromProto<ELayerAccessMethod>(layer.access_method()) == ELayerAccessMethod::Nbd) {
            auto deviceId = layer.GetRuntimeGuid();
            EmplaceOrCrash(NbdDeviceIds_, deviceId);
            ToProto(layer.mutable_nbd_device_id(), deviceId);
            ++nbdDeviceCount;
        }
    }

    // Mark artifacts that will be accessed via virtual layer and create virtual artifact key.
    if (HasVirtualSandboxArtifacts_) {
        TArtifactKey virtualArtifactKey;
        virtualArtifactKey.set_access_method(ToProto(NControllerAgent::ELayerAccessMethod::Nbd));
        virtualArtifactKey.set_filesystem(ToProto(NControllerAgent::ELayerFilesystem::SquashFS));
        {
            auto* dataSource = virtualArtifactKey.mutable_data_source();
            dataSource->set_type(ToProto(NChunkClient::EDataSourceType::File));
        }

        for (const auto& artifact : description->Artifacts) {
            if (artifact.AccessedViaVirtualSandbox) {
                // Construct concatenated data source path of the form "path1;path2;path3".
                auto* dataSource = virtualArtifactKey.mutable_data_source();
                if (dataSource->path().empty()) {
                    dataSource->set_path(artifact.Key.data_source().path());
                } else {
                    dataSource->set_path(Format("%v;%v", dataSource->path(), artifact.Key.data_source().path()));
                }

                for (const auto& chunkSpec : artifact.Key.chunk_specs()) {
                    virtualArtifactKey.add_chunk_specs()->CopyFrom(chunkSpec);
                }
            }
        }

        auto deviceId = virtualArtifactKey.GetRuntimeGuid();
        EmplaceOrCrash(NbdDeviceIds_, deviceId);
        ToProto(virtualArtifactKey.mutable_nbd_device_id(), deviceId);
        ++nbdDeviceCount;

        VirtualSandboxData_ = TVirtualSandboxData{
            .NbdDeviceId = deviceId,
            .ArtifactKey = std::move(virtualArtifactKey),
        };
    }

    // Create NBD device id for NBD root volume.
    if (description->SandboxNbdRootVolumeData) {
        auto deviceId = MakeNbdDeviceId(JobId_, nbdDeviceCount);
        EmplaceOrCrash(NbdDeviceIds_, deviceId);
        ++nbdDeviceCount;

        description->SandboxNbdRootVolumeData->DeviceId = std::move(deviceId);
    }
}

void TJobFSSecretary::ConfigureVolumes(TNonNullPtr<TJobFSDescription> description, const TUserJobSpec* userJobSpec, int userId)
{
    if (!userJobSpec) {
        return;
    }

    auto fromProtoVolumeMount = [&] (const TVolumeMountPtr& volumeMount, const NControllerAgent::NProto::TVolumeMount& protoVolumeMount) {
        std::filesystem::path mountPath(std::string(protoVolumeMount.mount_path()));
        if (!mountPath.is_absolute()) {
            if (description->RootVolumeLayerArtifactKeys.empty() || Bootstrap_->GetConfig()->ExecNode->JobProxy->TestRootFS) {
                volumeMount->MountPath = NFS::JoinPaths("/sandbox/", mountPath.string());
            } else {
                volumeMount->MountPath = NFS::JoinPaths("/slot/sandbox/", mountPath.string());
            }
        } else {
            volumeMount->MountPath = std::move(mountPath);
        }
        volumeMount->VolumeId = protoVolumeMount.volume_id();
        volumeMount->ReadOnly = protoVolumeMount.read_only();
    };

    std::optional<std::string_view> jobRootVolumeId;
    description->JobVolumeMounts.reserve(userJobSpec->job_volume_mounts().size());
    for (const auto& protoVolumeMount : userJobSpec->job_volume_mounts()) {
        auto volumeMount = New<TVolumeMount>();
        fromProtoVolumeMount(volumeMount, protoVolumeMount);

        if (volumeMount->MountPath == TAbsoluteNormalizedPath("/")) {
            jobRootVolumeId = volumeMount->VolumeId;
        }

        description->JobVolumeMounts.push_back(std::move(volumeMount));
    }
    std::sort(description->JobVolumeMounts.begin(), description->JobVolumeMounts.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs->MountPath < rhs->MountPath;
    });

    description->SidecarsVolumeMounts.reserve(userJobSpec->sidecars().size());
    for (const auto& [sidecarName, sidecar] : userJobSpec->sidecars()) {
        if (Bootstrap_->GetJobEnvironmentType() == NJobProxy::EJobEnvironmentType::Porto) {
            if (sidecar.has_docker_image()) {
                THROW_ERROR_EXCEPTION(
                    NExecNode::EErrorCode::DockerImagePullingFailed,
                    "External sidecar docker image is not supported in porto job environment");
            }
        } else if (Bootstrap_->GetJobEnvironmentType() == NJobProxy::EJobEnvironmentType::Cri) {
            if (!sidecar.sidecar_volume_mounts().empty()) {
                THROW_ERROR_EXCEPTION(
                    NExecNode::EErrorCode::LayerUnpackingFailed,
                    "Porto volumes are not supported in cri job environment");
            }
        }

        auto& sidecarVolumeMounts = description->SidecarsVolumeMounts[sidecarName];
        sidecarVolumeMounts.reserve(sidecar.sidecar_volume_mounts().size());

        for (const auto& protoVolumeMount : sidecar.sidecar_volume_mounts()) {
            auto volumeMount = New<TVolumeMount>();
            fromProtoVolumeMount(volumeMount, protoVolumeMount);
            sidecarVolumeMounts.push_back(std::move(volumeMount));
        }
        std::sort(sidecarVolumeMounts.begin(), sidecarVolumeMounts.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs->MountPath < rhs->MountPath;
        });
    }

    for (const auto& [volumeId, protoVolume] : userJobSpec->volumes()) {
        using TProtoMessage = NControllerAgent::NProto::TVolume;
        switch (protoVolume.disk_request_case()) {
            case TProtoMessage::DISK_REQUEST_NOT_SET:
                YT_VERIFY(volumeId == jobRootVolumeId);
                for (const auto& layerKey : protoVolume.layers()) {
                    description->RootVolumeLayerArtifactKeys.emplace_back(layerKey);
                }
                // Root volume reuse is only supported in Porto environment.
                description->RootVolumeAllowReusing = protoVolume.allow_reusing() &&
                    Bootstrap_->GetJobEnvironmentType() == NJobProxy::EJobEnvironmentType::Porto;

                break;
            case TProtoMessage::kLocalDiskRequest: {
                const auto& localDiskRequest = protoVolume.local_disk_request();
                if (jobRootVolumeId && jobRootVolumeId.value() == volumeId) {
                    description->RootVolumeDiskSpace = localDiskRequest.disk_request().storage_request_common_parameters().disk_space();
                    if (localDiskRequest.disk_request().has_inode_count()) {
                        description->RootVolumeInodeLimit = localDiskRequest.disk_request().inode_count();
                    }

                    for (const auto& layerKey : protoVolume.layers()) {
                        description->RootVolumeLayerArtifactKeys.emplace_back(layerKey);
                    }
                    // Root volume reuse is only supported in Porto environment.
                    description->RootVolumeAllowReusing = protoVolume.allow_reusing() &&
                        Bootstrap_->GetJobEnvironmentType() == NJobProxy::EJobEnvironmentType::Porto;
                } else {
                    auto localVolume = New<TLocalDiskVolumeParams>(volumeId, userId);
                    localVolume->Size = localDiskRequest.disk_request().storage_request_common_parameters().disk_space();
                    if (localDiskRequest.disk_request().has_inode_count()) {
                        localVolume->InodeLimit = localDiskRequest.disk_request().inode_count();
                    }
                    // Volume reuse is only supported in Porto environment.
                    localVolume->AllowReusing = protoVolume.allow_reusing() &&
                        Bootstrap_->GetJobEnvironmentType() == NJobProxy::EJobEnvironmentType::Porto;

                    for (const auto& layerKey : protoVolume.layers()) {
                        localVolume->LayerArtifactKeys.emplace_back(layerKey);
                    }
                    description->NonRootVolumeParams.push_back(std::move(localVolume));
                }
                break;
            }
            case TProtoMessage::kNbdDiskRequest: {
                TSandboxNbdRootVolumeData sandboxNbdData;
                NExecNode::FromProto(&sandboxNbdData, protoVolume.nbd_disk_request());
                description->SandboxNbdRootVolumeData = std::move(sandboxNbdData);

                for (const auto& layerKey : protoVolume.layers()) {
                    description->RootVolumeLayerArtifactKeys.emplace_back(layerKey);
                }
                break;
            }
            case TProtoMessage::kTmpfsStorageRequest: {
                auto tmpfsVolume = New<TTmpfsVolumeParams>(volumeId, userId);
                NExecNode::FromProto(tmpfsVolume.Get(), protoVolume.tmpfs_storage_request());
                // Volume reuse is only supported in Porto environment.
                tmpfsVolume->AllowReusing = protoVolume.allow_reusing() &&
                    Bootstrap_->GetJobEnvironmentType() == NJobProxy::EJobEnvironmentType::Porto;

                for (const auto& layerKey : protoVolume.layers()) {
                    tmpfsVolume->LayerArtifactKeys.emplace_back(layerKey);
                }

                description->NonRootVolumeParams.push_back(std::move(tmpfsVolume));
                break;
            }
        }
    }
}

const std::vector<TArtifactDescription>& TJobFSSecretary::GetArtifactDescriptors() const
{
    return Description_->Artifacts;
}

void TJobFSSecretary::MarkArtifactsAccessedViaVirtualSandbox(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec)
{
    if (!description->RootVolumeLayerArtifactKeys.empty() && RootVolumeDiskQuotaEnabled_) {
        for (auto& artifact : description->Artifacts) {
            if (CanBeAccessedViaVirtualSandbox(artifact, userJobSpec)) {
                artifact.AccessedViaVirtualSandbox = true;
                HasVirtualSandboxArtifacts_ = true;
            }
        }
    }
}

void TJobFSSecretary::MarkArtifactsAccessedViaBind(TNonNullPtr<TJobFSDescription> description)
{
    for (auto& artifact : description->Artifacts) {
        artifact.AccessedViaBind = CanBeAccessedViaBind(artifact);
    }
}

bool TJobFSSecretary::CanBeAccessedViaVirtualSandbox(
    const TArtifactDescription& artifact,
    const NControllerAgent::NProto::TUserJobSpec* userJobSpec) const
{
    if (artifact.BypassArtifactCache ||
        artifact.CopyFile ||
        artifact.Key.data_source().type() != ToProto(EDataSourceType::File))
    {
        return false;
    }

    for (const auto& tmpfsVolume : userJobSpec->tmpfs_volumes()) {
        if (tmpfsVolume.path() == "." || artifact.Name.starts_with(tmpfsVolume.path())) {
            return false;
        }
    }

    return true;
}

bool TJobFSSecretary::CanBeAccessedViaBind(const TArtifactDescription& artifact) const
{
    return !artifact.AccessedViaVirtualSandbox &&
        !artifact.BypassArtifactCache &&
        !artifact.CopyFile &&
        !Bootstrap_->GetConfig()->ExecNode->JobProxy->TestRootFS;
}

const std::vector<TArtifactKey>& TJobFSSecretary::GetRootVolumeLayerArtifactKeys() const
{
    return MergedRootVolumeLayerArtifactKeys_;
}

const std::vector<TArtifactKey>& TJobFSSecretary::GetGpuCheckVolumeLayerArtifactKeys() const
{
    return MergedGpuCheckVolumeLayerArtifactKeys_;
}

const std::optional<std::string>& TJobFSSecretary::GetDockerImage() const
{
    return ActualDockerImage_;
}

// Note that ActualDockerImage_ can be set multiple times during job preparation.
void TJobFSSecretary::SetDockerImage(std::optional<std::string> image)
{
    ActualDockerImage_ = std::move(image);
}

const std::optional<std::string>& TJobFSSecretary::GetDockerImageId() const
{
    return DockerImageId_;
}

void TJobFSSecretary::SetDockerImageId(std::optional<std::string> imageId)
{
    YT_VERIFY(!std::exchange(DockerImageId_, std::move(imageId)));
}

const IVolumePtr& TJobFSSecretary::GetRootVolume() const
{
    return RootVolume_;
}

void TJobFSSecretary::SetRootVolume(IVolumePtr volume)
{
    // Allow setting the same volume (reuse case) or setting a new volume when none exists.
    if (RootVolume_) {
        // Root volume is being reused - verify it's the same volume.
        YT_VERIFY(RootVolume_ == volume);
    } else {
        RootVolume_ = std::move(volume);
    }
}

IVolumePtr TJobFSSecretary::ReleaseRootVolume()
{
    YT_LOG_DEBUG("Releasing root volume");
    return std::move(RootVolume_);
}

const IVolumePtr& TJobFSSecretary::GetGpuCheckVolume() const
{
    return GpuCheckVolume_;
}

void TJobFSSecretary::SetGpuCheckVolume(IVolumePtr volume)
{
    YT_VERIFY(!std::exchange(GpuCheckVolume_, std::move(volume)));
}

IVolumePtr TJobFSSecretary::ReleaseGpuCheckVolume()
{
    YT_LOG_DEBUG("Releasing GPU check volume");
    return std::move(GpuCheckVolume_);
}

bool TJobFSSecretary::IsRootVolumeDiskQuotaEnabled() const
{
    return RootVolumeDiskQuotaEnabled_;
}

const THashSet<std::string>& TJobFSSecretary::GetNbdDeviceIds() const
{
    return NbdDeviceIds_;
}

THashSet<std::string> TJobFSSecretary::ReleaseNbdDeviceIds()
{
    YT_LOG_DEBUG("Releasing NBD device ids");
    return std::move(NbdDeviceIds_);
}

const std::optional<TSandboxNbdRootVolumeData>& TJobFSSecretary::GetSandboxNbdRootVolumeData() const
{
    return Description_->SandboxNbdRootVolumeData;
}

const THashMap<std::string, TVolumeResultPtr>& TJobFSSecretary::GetNonRootVolumes() const
{
    return NonRootVolumes_;
}

THashMap<std::string, TVolumeResultPtr> TJobFSSecretary::ReleaseNonReusableNonRootVolumes()
{
    YT_LOG_DEBUG("Releasing non-reusable non-root volumes");

    THashMap<std::string, TVolumeResultPtr> result;

    for (auto& [volumeId, volume] : NonRootVolumes_) {
        // Find the corresponding params to check if it's reusable.
        auto paramsIt = std::find_if(
            begin(Description_->NonRootVolumeParams),
            end(Description_->NonRootVolumeParams),
            [&volumeId = volumeId] (const TBaseVolumeParamsPtr& params) {
                return params->VolumeId == volumeId;
            });
        YT_VERIFY(paramsIt != Description_->NonRootVolumeParams.end());
        bool isReusable = (*paramsIt)->AllowReusing;
        if (!isReusable) {
            result[volumeId] = std::move(volume);
        }
    }

    for (const auto& [volumeId, _] : result) {
        NonRootVolumes_.erase(volumeId);
    }

    return result;
}

void TJobFSSecretary::SetNonRootVolumes(std::vector<TVolumeResultPtr> volumes)
{
    // Add newly prepared volumes to the existing ones (which may contain reusable volumes).
    for (auto& volume : volumes) {
        InsertOrCrash(NonRootVolumes_, std::pair(volume->VolumeId, std::move(volume)));
    }
}

std::vector<TBaseVolumeParamsPtr> TJobFSSecretary::GetNonRootVolumesToPrepare() const
{
    std::vector<TBaseVolumeParamsPtr> result;
    result.reserve(size(Description_->NonRootVolumeParams));
    for (const auto& params : Description_->NonRootVolumeParams) {
        // Check if this volume is already prepared (reused from previous job).
        if (!NonRootVolumes_.contains(params->VolumeId)) {
            // Volume not yet prepared, needs preparation.
            result.push_back(params);
        }
    }
    return result;
}

size_t TJobFSSecretary::GetTmpfsVolumeCount() const
{
    i64 tmpfsVolumeCount = 0;
    for (const auto& [volumeId, volume] : NonRootVolumes_) {
        if (volume->VolumeType == EVolumeType::Tmpfs) {
            ++tmpfsVolumeCount;
        }
    }
    return tmpfsVolumeCount;
}

const std::optional<TVirtualSandboxData>& TJobFSSecretary::GetVirtualSandboxData() const
{
    return VirtualSandboxData_;
}

void TJobFSSecretary::SetVirtualSandboxReader(NNbd::NImage::IImageReaderPtr reader)
{
    YT_VERIFY(VirtualSandboxData_);
    VirtualSandboxData_->Reader = std::move(reader);
}

const std::optional<i64>& TJobFSSecretary::GetRootVolumeDiskSpace() const
{
    return Description_->RootVolumeDiskSpace;
}

const std::optional<i64>& TJobFSSecretary::GetRootVolumeInodeLimit() const
{
    return Description_->RootVolumeInodeLimit;
}

bool TJobFSSecretary::IsRootVolumeReusable() const
{
    return Description_->RootVolumeAllowReusing;
}

IVolumePtr TJobFSSecretary::ReleaseRootVolumeIfNeeded()
{
    if (Description_->RootVolumeAllowReusing) {
        return nullptr;
    }
    return std::move(RootVolume_);
}

std::vector<IVolumePtr> TJobFSSecretary::ReleaseVolumes()
{
    YT_LOG_DEBUG("Releasing volumes");

    std::vector<IVolumePtr> volumes;
    volumes.reserve(2 + NonRootVolumes_.size());

    volumes.push_back(std::move(RootVolume_));
    volumes.push_back(std::move(GpuCheckVolume_));

    for (auto& [_, volumeResult] : NonRootVolumes_) {
        volumes.push_back(std::move(volumeResult->Volume));
    }
    NonRootVolumes_.clear();

    return volumes;
}

const std::vector<TBaseVolumeParamsPtr>& TJobFSSecretary::GetNonRootVolumeParams() const
{
    return Description_->NonRootVolumeParams;
}

const std::vector<TVolumeMountPtr>& TJobFSSecretary::GetJobVolumeMounts() const
{
    return Description_->JobVolumeMounts;
}

const THashMap<std::string, std::vector<TVolumeMountPtr>>& TJobFSSecretary::GetSidecarsVolumeMounts() const
{
    return Description_->SidecarsVolumeMounts;
}

const TArtifactPtr& TJobFSSecretary::GetArtifactByName(const std::string& name) const
{
    return GetOrCrash(NameToPreparedArtifacts_, name);
}

const TArtifactDescription& TJobFSSecretary::GetUserArtifactDescriptor(const std::string& name) const
{
    int index = GetOrCrash(Description_->UserArtifactNameToIndex, name);
    return Description_->Artifacts[index];
}

std::vector<TArtifactDescription> TJobFSSecretary::GetArtifactsToCache() const
{
    if (ArtifactsCached_) {
        return {};
    }

    std::vector<TArtifactDescription> result;
    result.reserve(size(Description_->Artifacts));
    for (const auto& artifact : Description_->Artifacts) {
        if (!artifact.BypassArtifactCache && !artifact.AccessedViaVirtualSandbox) {
            result.push_back(artifact);
        }
    }
    return result;
}

void TJobFSSecretary::SetCachedArtifacts(std::vector<TArtifactPtr> artifacts)
{
    if (empty(artifacts)) {
        return;
    }

    int cacheIndex = 0;
    for (auto& artifact : Description_->Artifacts) {
        if (!artifact.BypassArtifactCache && !artifact.AccessedViaVirtualSandbox) {
            YT_VERIFY(cacheIndex < ssize(artifacts));
            NameToPreparedArtifacts_[artifact.Name] = std::move(artifacts[cacheIndex++]);
        }
    }
    YT_VERIFY(cacheIndex == ssize(artifacts));
    ArtifactsCached_ = true;
}

void TJobFSSecretary::AddPreparedLayers(TPreparedLayers layers)
{
    YT_LOG_DEBUG(
        "Adding prepared layers (NewLayerCount: %v, ExistingLayerCount: %v)",
        size(layers.ArtifactKeyToOverlayData),
        size(PreparedLayers_.ArtifactKeyToOverlayData));

    for (auto& [key, overlayData] : layers.ArtifactKeyToOverlayData) {
        EmplaceOrCrash(
            PreparedLayers_.ArtifactKeyToOverlayData,
            std::move(key),
            std::move(overlayData));
    }
}

bool TJobFSSecretary::HasPreparedLayer(const TArtifactKey& key) const
{
    return PreparedLayers_.ArtifactKeyToOverlayData.contains(key);
}

std::vector<TOverlayData> TJobFSSecretary::GetPreparedRootVolumeOverlayData() const
{
    auto overlayDataArray = GetPreparedOverlayData(MergedRootVolumeLayerArtifactKeys_);
    if (VirtualSandboxData_) {
        overlayDataArray.push_back(GetOrCrash(
            PreparedLayers_.ArtifactKeyToOverlayData,
            VirtualSandboxData_->ArtifactKey));
    }
    return overlayDataArray;
}

std::vector<TOverlayData> TJobFSSecretary::GetPreparedGpuCheckVolumeOverlayData() const
{
    return GetPreparedOverlayData(MergedGpuCheckVolumeLayerArtifactKeys_);
}

std::vector<TOverlayData> TJobFSSecretary::GetPreparedNonRootVolumeOverlayData(const TBaseVolumeParams& params) const
{
    return GetPreparedOverlayData(params.LayerArtifactKeys);
}

std::vector<TOverlayData> TJobFSSecretary::GetPreparedOverlayData(const std::vector<TArtifactKey>& artifactKeys) const
{
    std::vector<TOverlayData> overlayDataArray;
    overlayDataArray.reserve(size(artifactKeys));
    for (const auto& key : artifactKeys) {
        overlayDataArray.push_back(GetOrCrash(PreparedLayers_.ArtifactKeyToOverlayData, key));
    }
    return overlayDataArray;
}

void TJobFSSecretary::ReleasePreparedLayers()
{
    YT_LOG_DEBUG(
        "Releasing prepared layers (LayerCount: %v)",
        size(PreparedLayers_.ArtifactKeyToOverlayData));
    PreparedLayers_ = {};
}

void TJobFSSecretary::ReleaseArtifacts()
{
    YT_LOG_DEBUG("Releasing artifacts");

    NameToPreparedArtifacts_.clear();
}

void TJobFSSecretary::OnNewJobStarted(TJobId jobId)
{
    JobId_ = jobId;
    Logger = BaseLogger_.WithTag("JobId: %v", jobId);

    RootVolumeDiskQuotaEnabled_ = false;
    DockerImageId_.reset();
    // Verify that root volume is reusable if it's still present.
    // Non-reusable root volume should have been released via ReleaseRootVolumeIfNeeded().
    YT_VERIFY(!RootVolume_ || Description_->RootVolumeAllowReusing);
    GpuCheckVolume_.Reset();
    NbdDeviceIds_.clear();

    // Verify that all remaining non-root volumes are reusable.
    // Non-reusable volumes should have been released via ReleaseNonReusableNonRootVolumes().
    for (const auto& [volumeId, volume] : NonRootVolumes_) {
        auto paramsIt = std::find_if(
            Description_->NonRootVolumeParams.begin(),
            Description_->NonRootVolumeParams.end(),
            [&volumeId = volumeId] (const TBaseVolumeParamsPtr& params) {
                return params->VolumeId == volumeId;
            });
        YT_VERIFY(paramsIt != Description_->NonRootVolumeParams.end() && (*paramsIt)->AllowReusing);
    }

    VirtualSandboxData_.reset();
    HasVirtualSandboxArtifacts_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
