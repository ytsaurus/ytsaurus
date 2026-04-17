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

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString MakeNbdDeviceId(TJobId jobId, int nbdDeviceIndex)
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
    const std::optional<TString>& baseline,
    const std::optional<TString>& current)
{
    if (baseline != current) {
        return TError("Job spec docker image differs from the first job in this allocation")
            << TErrorAttribute("baseline", baseline)
            << TErrorAttribute("current", current);
    }
    return {};
}

TError CheckRootVolumeDiskSpaceAndInodeLimit(
    std::optional<int> baselineDiskSpace,
    std::optional<int> currentDiskSpace,
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
    for (int i = 0; i < ssize(current); ++i) {
        if (!(*baseline[i] == *current[i])) {
            return TError("Job spec non-root volume params differ from the first job in this allocation")
                << TErrorAttribute("volume_index", i)
                << TErrorAttribute("baseline", Format("%v", baseline[i]))
                << TErrorAttribute("current", Format("%v", current[i]));
        }
    }
    return {};
}

TError CheckJobVolumeMounts(
    const std::vector<NScheduler::TVolumeMountPtr>& baseline,
    const std::vector<NScheduler::TVolumeMountPtr>& current)
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

    // Build description directly into a local struct so the result
    // does not depend on any prior member-variable state.
    TJobFSDescription current;
    ConfigureUserArtifacts(GetPtr(current), userJobSpec);
    ConfigureLayerArtifacts(GetPtr(current), userJobSpec);
    ConfigureDockerImage(GetPtr(current), userJobSpec);
    ConfigureUdfArtifacts(GetPtr(current), jobSpecExt);
    ConfigureVolumes(GetPtr(current), userJobSpec, userId);

    // Baseline verification:
    //  - after all spec-derived Configure* calls,
    //  - before AddGpuToppingLayersIfNeeded (adds node-derived layers from GpuManager),
    //  - before MarkArtifactsAccessed* (set AccessedViaBind/AccessedViaVirtualSandbox
    //    depending on hasNbdServer, enableVirtualSandbox, etc.).
    // For the first job the member fields are not yet set, so we just apply.
    // For subsequent jobs the member fields still hold the first job's data
    // (OnNewJobStarted does not clear them), so we compare against them.
    if (!firstConfiguration) {
        VerifyDescriptionMatchesApplied(current);
    } else {
        ApplyDescription(GetPtr(current));
    }

    if (needGpuLayers && userJobSpec) {
        AddGpuToppingLayersIfNeeded(userJobSpec);
    }
    if (enableVirtualSandbox && hasNbdServer) {
        MarkArtifactsAccessedViaVirtualSandbox(userJobSpec);
    }
    MarkArtifactsAccessedViaBind();

    CheckConfiguration(hasNbdServer);

    ConfigureNbdDeviceIds();
}

void TJobFSSecretary::VerifyDescriptionMatchesApplied(const TJobFSDescription& current) const
{
    auto crashIfFailed = [&] (const TError& error) {
        YT_LOG_FATAL_UNLESS(
            error.IsOK(),
            error,
            "Job spec differs from the first job in this allocation (CurrentJobId: %v)",
            JobId_);
    };

    crashIfFailed(CheckArtifacts(Artifacts_, current.Artifacts));
    crashIfFailed(CheckLayerArtifactKeys(
        RootVolumeLayerArtifactKeys_,
        current.RootVolumeLayerArtifactKeys,
        GpuCheckVolumeLayerArtifactKeys_,
        current.GpuCheckVolumeLayerArtifactKeys));
    crashIfFailed(CheckDockerImage(DockerImage_, current.DockerImage));

    crashIfFailed(CheckRootVolumeDiskSpaceAndInodeLimit(
            RootVolumeDiskSpace_,
            current.RootVolumeDiskSpace,
            RootVolumeInodeLimit_,
            current.RootVolumeInodeLimit));
    crashIfFailed(CheckNonRootVolumeParams(NonRootVolumeParams_, current.NonRootVolumeParams));
    crashIfFailed(CheckJobVolumeMounts(JobVolumeMounts_, current.JobVolumeMounts));
    crashIfFailed(CheckSandboxNbdRootVolumeData(SandboxNbdRootVolumeData_, current.SandboxNbdRootVolumeData));
}

void TJobFSSecretary::ApplyDescription(TNonNullPtr<TJobFSDescription> description)
{
    Artifacts_ = std::move(description->Artifacts);
    UserArtifactNameToIndex_ = std::move(description->UserArtifactNameToIndex);
    RootVolumeLayerArtifactKeys_ = std::move(description->RootVolumeLayerArtifactKeys);
    GpuCheckVolumeLayerArtifactKeys_ = std::move(description->GpuCheckVolumeLayerArtifactKeys);
    DockerImage_ = std::move(description->DockerImage);
    RootVolumeDiskSpace_ = description->RootVolumeDiskSpace;
    RootVolumeInodeLimit_ = description->RootVolumeInodeLimit;
    NonRootVolumeParams_ = std::move(description->NonRootVolumeParams);
    JobVolumeMounts_ = std::move(description->JobVolumeMounts);
    SandboxNbdRootVolumeData_ = std::move(description->SandboxNbdRootVolumeData);
}

void TJobFSSecretary::CheckConfiguration(bool hasNbdServer) const
{
    if (SandboxNbdRootVolumeData_ && !hasNbdServer) {
        THROW_ERROR_EXCEPTION(
            NExecNode::EErrorCode::NbdServerDisabledOnNode,
            "Nbd server disabled on this node but job requested nbd volume");
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
            .Artifact = nullptr,
        });
        EmplaceOrCrash(description->UserArtifactNameToIndex, descriptor.file_name(), description->Artifacts.size() - 1);
    }
}

void TJobFSSecretary::AddGpuToppingLayersIfNeeded(const TUserJobSpec* userJobSpec)
{
    if (!userJobSpec->enable_gpu_layers()) {
        return;
    }

    if (userJobSpec->root_volume_layers().empty()) {
        THROW_ERROR_EXCEPTION(
            NExecNode::EErrorCode::GpuJobWithoutLayers,
            "No layers specified for GPU job; at least a base layer is required to use GPU");
    }

    auto toppingLayers = Bootstrap_->GetGpuManager()->GetToppingLayers();

    if (!userJobSpec->gpu_check_volume_layers().empty()) {
        GpuCheckVolumeLayerArtifactKeys_.insert(
            GpuCheckVolumeLayerArtifactKeys_.begin(),
            toppingLayers.begin(),
            toppingLayers.end());
    }

    RootVolumeLayerArtifactKeys_.insert(
            RootVolumeLayerArtifactKeys_.begin(),
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
            .Artifact = nullptr,
        });
    }
}

void TJobFSSecretary::ConfigureNbdDeviceIds()
{
    // Mark NBD layers with NBD device ids.
    int nbdDeviceCount = 0;
    for (auto& layer : RootVolumeLayerArtifactKeys_) {
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

        for (auto& artifact : Artifacts_) {
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
    if (SandboxNbdRootVolumeData_) {
        auto deviceId = MakeNbdDeviceId(JobId_, nbdDeviceCount);
        EmplaceOrCrash(NbdDeviceIds_, deviceId);
        ++nbdDeviceCount;

        SandboxNbdRootVolumeData_->DeviceId = std::move(deviceId);
    }
}

void TJobFSSecretary::ConfigureVolumes(TNonNullPtr<TJobFSDescription> description, const TUserJobSpec* userJobSpec, int userId)
{
    if (!userJobSpec) {
        return;
    }

    std::optional<std::string_view> rootVolumeId;
    description->JobVolumeMounts.reserve(userJobSpec->job_volume_mounts().size());
    for (const auto& protoVolumeMount : userJobSpec->job_volume_mounts()) {
        auto volumeMount = New<NScheduler::TVolumeMount>();
        FromProto(volumeMount.Get(), protoVolumeMount);
        if (volumeMount->MountPath == "/") {
            rootVolumeId = volumeMount->VolumeId;
        }
        description->JobVolumeMounts.push_back(std::move(volumeMount));
    }

    for (const auto& [volumeId, protoVolume] : userJobSpec->volumes()) {
        using TProtoMessage = NControllerAgent::NProto::TVolume;
        switch (protoVolume.disk_request_case()) {
            case TProtoMessage::DISK_REQUEST_NOT_SET:
                for (const auto& layerKey : protoVolume.layers()) {
                    description->RootVolumeLayerArtifactKeys.emplace_back(layerKey);
                }

                break;
            case TProtoMessage::kLocalDiskRequest: {
                const auto& localDiskRequest = protoVolume.local_disk_request();
                if (rootVolumeId && rootVolumeId.value() == volumeId) {
                    description->RootVolumeDiskSpace = localDiskRequest.disk_request().storage_request_common_parameters().disk_space();
                    if (localDiskRequest.disk_request().has_inode_count()) {
                        description->RootVolumeInodeLimit = localDiskRequest.disk_request().inode_count();
                    }

                    for (const auto& layerKey : protoVolume.layers()) {
                        description->RootVolumeLayerArtifactKeys.emplace_back(layerKey);
                    }
                } else {
                    auto localVolume = New<TLocalDiskVolumeParams>(volumeId, userId);
                    localVolume->Size = localDiskRequest.disk_request().storage_request_common_parameters().disk_space();
                    if (localDiskRequest.disk_request().has_inode_count()) {
                        localVolume->InodeLimit = localDiskRequest.disk_request().inode_count();
                    }

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

                for (const auto& layerKey : protoVolume.layers()) {
                    tmpfsVolume->LayerArtifactKeys.emplace_back(layerKey);
                }

                description->NonRootVolumeParams.push_back(std::move(tmpfsVolume));
                break;
            }
        }
    }
}

const std::vector<TArtifactDescription>& TJobFSSecretary::GetArtifacts() const
{
    return Artifacts_;
}

void TJobFSSecretary::MarkArtifactsAccessedViaVirtualSandbox(const NControllerAgent::NProto::TUserJobSpec* userJobSpec)
{
    if (!RootVolumeLayerArtifactKeys_.empty() && RootVolumeDiskQuotaEnabled_) {
        for (auto& artifact : Artifacts_) {
            if (CanBeAccessedViaVirtualSandbox(artifact, userJobSpec)) {
                artifact.AccessedViaVirtualSandbox = true;
                HasVirtualSandboxArtifacts_ = true;
            }
        }
    }
}

void TJobFSSecretary::MarkArtifactsAccessedViaBind()
{
    for (auto& artifact : Artifacts_) {
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
        if (tmpfsVolume.path() == "." || artifact.Name.StartsWith(tmpfsVolume.path())) {
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
    return RootVolumeLayerArtifactKeys_;
}

const std::vector<TArtifactKey>& TJobFSSecretary::GetGpuCheckVolumeLayerArtifactKeys() const
{
    return GpuCheckVolumeLayerArtifactKeys_;
}

const std::optional<TString>& TJobFSSecretary::GetDockerImage() const
{
    return DockerImage_;
}

// Note that DockerImage_ can be set multiple times during job preparation.
void TJobFSSecretary::SetDockerImage(std::optional<TString> image)
{
    DockerImage_ = std::move(image);
}

const std::optional<TString>& TJobFSSecretary::GetDockerImageId() const
{
    return DockerImageId_;
}

void TJobFSSecretary::SetDockerImageId(std::optional<TString> imageId)
{
    YT_VERIFY(!std::exchange(DockerImageId_, std::move(imageId)));
}

const IVolumePtr& TJobFSSecretary::GetRootVolume() const
{
    return RootVolume_;
}

void TJobFSSecretary::SetRootVolume(IVolumePtr volume)
{
    YT_VERIFY(!std::exchange(RootVolume_, std::move(volume)));
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

const THashSet<TString>& TJobFSSecretary::GetNbdDeviceIds() const
{
    return NbdDeviceIds_;
}

THashSet<TString> TJobFSSecretary::ReleaseNbdDeviceIds()
{
    YT_LOG_DEBUG("Releasing NBD device ids");
    return std::move(NbdDeviceIds_);
}

const std::optional<TSandboxNbdRootVolumeData>& TJobFSSecretary::GetSandboxNbdRootVolumeData() const
{
    return SandboxNbdRootVolumeData_;
}

const std::vector<TVolumeResultPtr>& TJobFSSecretary::GetNonRootVolumes() const
{
    return NonRootVolumes_;
}

std::vector<TVolumeResultPtr> TJobFSSecretary::ReleaseNonRootVolumes()
{
    YT_LOG_DEBUG("Releasing non-root volumes");
    return std::move(NonRootVolumes_);
}

void TJobFSSecretary::SetNonRootVolumes(std::vector<TVolumeResultPtr> volumes)
{
    NonRootVolumes_ = std::move(volumes);
}

size_t TJobFSSecretary::GetTmpfsVolumeCount() const
{
    i64 tmpfsVolumeCount = 0;
    for (const auto& volume : NonRootVolumes_) {
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

void TJobFSSecretary::SetVirtualSandboxReader(NNbd::IImageReaderPtr reader)
{
    YT_VERIFY(VirtualSandboxData_);
    VirtualSandboxData_->Reader = std::move(reader);
}

const std::optional<int>& TJobFSSecretary::GetRootVolumeDiskSpace() const
{
    return RootVolumeDiskSpace_;
}

const std::optional<int64_t>& TJobFSSecretary::GetRootVolumeInodeLimit() const
{
    return RootVolumeInodeLimit_;
}

const std::vector<TBaseVolumeParamsPtr>& TJobFSSecretary::GetNonRootVolumeParams() const
{
    return NonRootVolumeParams_;
}

const std::vector<NScheduler::TVolumeMountPtr>& TJobFSSecretary::GetJobVolumeMounts() const
{
    return JobVolumeMounts_;
}

const TArtifactDescription& TJobFSSecretary::GetUserArtifact(const TString& name) const
{
    int index = GetOrCrash(UserArtifactNameToIndex_, name);
    return Artifacts_[index];
}

std::vector<TArtifactDescription> TJobFSSecretary::GetArtifactsToCache() const
{
    if (ArtifactsCached_) {
        return {};
    }

    std::vector<TArtifactDescription> result;
    result.reserve(size(Artifacts_));
    for (const auto& artifact : Artifacts_) {
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
    for (auto& artifact : Artifacts_) {
        if (!artifact.BypassArtifactCache && !artifact.AccessedViaVirtualSandbox) {
            YT_VERIFY(cacheIndex < ssize(artifacts));
            artifact.Artifact = std::move(artifacts[cacheIndex++]);
        }
    }
    YT_VERIFY(cacheIndex == ssize(artifacts));
    ArtifactsCached_ = true;
}

void TJobFSSecretary::ReleaseArtifacts()
{
    YT_LOG_DEBUG("Releasing artifacts");

    for (auto& artifact : Artifacts_) {
        artifact.Artifact.Reset();
    }
}

void TJobFSSecretary::OnNewJobStarted(TJobId jobId)
{
    JobId_ = jobId;
    Logger = BaseLogger_.WithTag("JobId: %v", jobId);

    RootVolumeDiskQuotaEnabled_ = false;
    DockerImageId_.reset();
    RootVolume_.Reset();
    GpuCheckVolume_.Reset();
    NbdDeviceIds_.clear();
    NonRootVolumes_.clear();
    VirtualSandboxData_.reset();
    HasVirtualSandboxArtifacts_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
