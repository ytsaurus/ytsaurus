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

TJobFSSecretary::TJobFSSecretary(
    IBootstrap* bootstrap,
    NLogging::TLogger logger,
    bool tmpfsEnabled)
    : Bootstrap_(bootstrap)
    , BaseLogger_(std::move(logger))
    , Logger(BaseLogger_)
    , TmpfsEnabled_(tmpfsEnabled)
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
    OnNewJobStarted(jobId);
    RootVolumeDiskQuotaEnabled_ = enableRootVolumeDiskQuota;

    ConfigureUserArtifacts(userJobSpec);
    if (needGpuLayers && userJobSpec) {
        AddGpuToppingLayersIfNeeded(userJobSpec);
    }
    ConfigureLayerArtifacts(userJobSpec);
    ConfigureDockerImage(userJobSpec);
    ConfigureUdfArtifacts(jobSpecExt);

    if (enableVirtualSandbox && hasNbdServer) {
        MarkArtifactsAccessedViaVirtualSandbox(userJobSpec);
    }
    MarkArtifactsAccessedViaBind();

    ConfigureVolumes(userJobSpec, userId, hasNbdServer);

    ConfigureNbdDeviceIds();
}

void TJobFSSecretary::ConfigureUserArtifacts(const TUserJobSpec* userJobSpec)
{
    if (!userJobSpec) {
        return;
    }

    for (const auto& descriptor : userJobSpec->files()) {
        Artifacts_.push_back(TArtifactDescription{
            .SandboxKind = ESandboxKind::User,
            .Name = descriptor.file_name(),
            .Executable = descriptor.executable(),
            .BypassArtifactCache = descriptor.bypass_artifact_cache(),
            .CopyFile = descriptor.copy_file(),
            .Key = TArtifactKey(descriptor),
            .Artifact = nullptr,
        });
        EmplaceOrCrash(UserArtifactNameToIndex_, descriptor.file_name(), Artifacts_.size() - 1);
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
    for (auto& layer : toppingLayers) {
        if (!userJobSpec->root_volume_layers().empty()) {
            GpuCheckVolumeLayerArtifactKeys_.push_back(layer);
        }
        RootVolumeLayerArtifactKeys_.push_back(std::move(layer));
    }
}

void TJobFSSecretary::ConfigureLayerArtifacts(const TUserJobSpec* userJobSpec)
{
    if (!userJobSpec) {
        return;
    }

    for (const auto& layerKey : userJobSpec->root_volume_layers()) {
        RootVolumeLayerArtifactKeys_.emplace_back(layerKey);
    }

    for (const auto& layerKey : userJobSpec->gpu_check_volume_layers()) {
        GpuCheckVolumeLayerArtifactKeys_.emplace_back(layerKey);
    }
}

void TJobFSSecretary::ConfigureDockerImage(const TUserJobSpec* userJobSpec)
{
    if (userJobSpec && userJobSpec->has_docker_image()) {
        DockerImage_ = userJobSpec->docker_image();
    }
}

void TJobFSSecretary::ConfigureUdfArtifacts(const TJobSpecExt& jobSpecExt)
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

        Artifacts_.push_back(TArtifactDescription{
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

void TJobFSSecretary::ConfigureVolumes(const TUserJobSpec* userJobSpec, int userId, bool hasNbdServer)
{
    if (!userJobSpec) {
        return;
    }

    JobVolumeMounts_.reserve(userJobSpec->job_volume_mounts().size());
    for (const auto& protoVolumeMount : userJobSpec->job_volume_mounts()) {
        auto volumeMount = New<NScheduler::TVolumeMount>();
        FromProto(volumeMount.Get(), protoVolumeMount);
        JobVolumeMounts_.push_back(std::move(volumeMount));
    }

    for (const auto& [volumeId, protoVolume] : userJobSpec->volumes()) {
        using TProtoMessage = NControllerAgent::NProto::TVolume;
        switch (protoVolume.disk_request_case()) {
            case TProtoMessage::DISK_REQUEST_NOT_SET:
                break;
            case TProtoMessage::kLocalDiskRequest: {
                const auto& localDiskRequest = protoVolume.local_disk_request();
                RootVolumeDiskSpace_ = localDiskRequest.disk_request().storage_request_common_parameters().disk_space();
                if (localDiskRequest.disk_request().has_inode_count()) {
                    RootVolumeInodeLimit_ = localDiskRequest.disk_request().inode_count();
                }
                break;
            }
            case TProtoMessage::kNbdDiskRequest: {
                if (!hasNbdServer) {
                    THROW_ERROR_EXCEPTION(
                        NExecNode::EErrorCode::NbdServerDisabledOnNode,
                        "Nbd server disabled on this node but job requested nbd volume");
                }
                TSandboxNbdRootVolumeData sandboxNbdData;
                NExecNode::FromProto(&sandboxNbdData, protoVolume.nbd_disk_request());
                SandboxNbdRootVolumeData_ = std::move(sandboxNbdData);
                break;
            }
            case TProtoMessage::kTmpfsStorageRequest: {
                TTmpfsVolumeParams tmpfsVolume;
                NExecNode::FromProto(&tmpfsVolume, protoVolume.tmpfs_storage_request());
                tmpfsVolume.UserId = userId;
                tmpfsVolume.VolumeId = volumeId;
                TmpfsVolumeParams_.push_back(std::move(tmpfsVolume));
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

    if (!TmpfsEnabled_) {
        return true;
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

const std::vector<TTmpfsVolumeResult>& TJobFSSecretary::GetTmpfsVolumes() const
{
    return TmpfsVolumes_;
}

std::vector<TTmpfsVolumeResult> TJobFSSecretary::ReleaseTmpfsVolumes()
{
    YT_LOG_DEBUG("Releasing tmpfs volumes");
    return std::move(TmpfsVolumes_);
}

void TJobFSSecretary::SetTmpfsVolumes(std::vector<TTmpfsVolumeResult> volumes)
{
    TmpfsVolumes_ = std::move(volumes);
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

const std::vector<TTmpfsVolumeParams>& TJobFSSecretary::GetTmpfsVolumeParams() const
{
    return TmpfsVolumeParams_;
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

std::vector<TArtifactKey> TJobFSSecretary::GetArtifactsToCache() const
{
    std::vector<TArtifactKey> result;
    for (const auto& artifact : Artifacts_) {
        if (!artifact.BypassArtifactCache && !artifact.AccessedViaVirtualSandbox) {
            result.push_back(artifact.Key);
        }
    }
    return result;
}

void TJobFSSecretary::SetCachedArtifacts(std::vector<TArtifactPtr> artifacts)
{
    YT_VERIFY(artifacts.size() == Artifacts_.size());
    for (size_t i = 0; i < Artifacts_.size(); ++i) {
        Artifacts_[i].Artifact = std::move(artifacts[i]);
    }
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
    Artifacts_.clear();
    RootVolumeLayerArtifactKeys_.clear();
    GpuCheckVolumeLayerArtifactKeys_.clear();
    DockerImage_.reset();
    DockerImageId_.reset();
    RootVolume_.Reset();
    GpuCheckVolume_.Reset();
    NbdDeviceIds_.clear();
    SandboxNbdRootVolumeData_.reset();
    UserArtifactNameToIndex_.clear();
    TmpfsVolumes_.clear();
    VirtualSandboxData_.reset();
    RootVolumeDiskSpace_.reset();
    RootVolumeInodeLimit_.reset();
    TmpfsVolumeParams_.clear();
    JobVolumeMounts_.clear();
    HasVirtualSandboxArtifacts_ = false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
