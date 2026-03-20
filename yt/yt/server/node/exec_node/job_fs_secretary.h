#pragma once

#include "artifact_description.h"
#include "preparation_options.h"
#include "public.h"
#include "private.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! Stores job artifacts and layer keys (does not prepare or download them).
class TJobFSSecretary
    : public TRefCounted
{
public:
    TJobFSSecretary(
        IBootstrap* bootstrap,
        NLogging::TLogger logger,
        bool tmpfsEnabled);

    //! Must be called before any other methods during job preparation.
    void ConfigureFromSpec(
        TJobId jobId,
        const NControllerAgent::NProto::TJobSpecExt& jobSpecExt,
        const NControllerAgent::NProto::TUserJobSpec* userJobSpec,
        int userId,
        bool hasNbdServer,
        bool enableVirtualSandbox,
        bool enableRootVolumeDiskQuota,
        bool needGpuLayers);

    const std::vector<TArtifactDescription>& GetArtifacts() const;

    const std::vector<TArtifactKey>& GetRootVolumeLayerArtifactKeys() const;
    const std::vector<TArtifactKey>& GetGpuCheckVolumeLayerArtifactKeys() const;

    const std::optional<TString>& GetDockerImage() const;
    void SetDockerImage(std::optional<TString> image);

    const std::optional<TString>& GetDockerImageId() const;
    void SetDockerImageId(std::optional<TString> imageId);

    const IVolumePtr& GetRootVolume() const;
    void SetRootVolume(IVolumePtr volume);
    IVolumePtr ReleaseRootVolume();

    const IVolumePtr& GetGpuCheckVolume() const;
    void SetGpuCheckVolume(IVolumePtr volume);
    IVolumePtr ReleaseGpuCheckVolume();

    bool IsRootVolumeDiskQuotaEnabled() const;

    const THashSet<TString>& GetNbdDeviceIds() const;
    THashSet<TString> ReleaseNbdDeviceIds();

    const std::optional<TSandboxNbdRootVolumeData>& GetSandboxNbdRootVolumeData() const;

    const std::vector<TTmpfsVolumeResult>& GetTmpfsVolumes() const;
    std::vector<TTmpfsVolumeResult> ReleaseTmpfsVolumes();
    void SetTmpfsVolumes(std::vector<TTmpfsVolumeResult> volumes);

    const std::optional<TVirtualSandboxData>& GetVirtualSandboxData() const;
    void SetVirtualSandboxReader(NNbd::IImageReaderPtr reader);

    const std::optional<int>& GetRootVolumeDiskSpace() const;
    const std::optional<int64_t>& GetRootVolumeInodeLimit() const;

    const std::vector<TTmpfsVolumeParams>& GetTmpfsVolumeParams() const;

    const std::vector<NScheduler::TVolumeMountPtr>& GetJobVolumeMounts() const;

    const TArtifactDescription& GetUserArtifact(const TString& name) const;

    //! Excludes artifacts that bypass cache or are accessed via virtual sandbox.
    std::vector<TArtifactKey> GetArtifactsToCache() const;

    //! The size of artifacts must match the size of Artifacts_.
    void SetCachedArtifacts(std::vector<TArtifactPtr> artifacts);

    void ReleaseArtifacts();

private:
    TJobId JobId_;
    IBootstrap* const Bootstrap_;
    const NLogging::TLogger BaseLogger_;
    NLogging::TLogger Logger;
    bool RootVolumeDiskQuotaEnabled_ = false;
    const bool TmpfsEnabled_;

    std::vector<TArtifactDescription> Artifacts_;
    std::vector<TArtifactKey> RootVolumeLayerArtifactKeys_;
    std::vector<TArtifactKey> GpuCheckVolumeLayerArtifactKeys_;
    std::optional<TString> DockerImage_;
    std::optional<TString> DockerImageId_;
    IVolumePtr RootVolume_;
    IVolumePtr GpuCheckVolume_;
    THashSet<TString> NbdDeviceIds_;
    std::optional<TSandboxNbdRootVolumeData> SandboxNbdRootVolumeData_;
    THashMap<TString, int> UserArtifactNameToIndex_;
    std::vector<TTmpfsVolumeResult> TmpfsVolumes_;
    std::optional<TVirtualSandboxData> VirtualSandboxData_;
    // COMPAT(krasovav)
    std::optional<int> RootVolumeDiskSpace_;
    // COMPAT(krasovav)
    std::optional<int64_t> RootVolumeInodeLimit_;
    std::vector<TTmpfsVolumeParams> TmpfsVolumeParams_;
    std::vector<NScheduler::TVolumeMountPtr> JobVolumeMounts_;
    bool HasVirtualSandboxArtifacts_ = false;

    void ConfigureUserArtifacts(const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureLayerArtifacts(const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureDockerImage(const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureUdfArtifacts(const NControllerAgent::NProto::TJobSpecExt& jobSpecExt);
    void ConfigureNbdDeviceIds();
    void ConfigureVolumes(const NControllerAgent::NProto::TUserJobSpec* userJobSpec, int userId, TJobId jobId, bool hasNbdServer);

    void MarkArtifactsAccessedViaVirtualSandbox(const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void MarkArtifactsAccessedViaBind();
    bool CanBeAccessedViaVirtualSandbox(const TArtifactDescription& artifact, const NControllerAgent::NProto::TUserJobSpec* userJobSpec) const;
    bool CanBeAccessedViaBind(const TArtifactDescription& artifact) const;

    void OnNewJobStarted(TJobId jobId);

    void AddGpuToppingLayersIfNeeded(const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
};

DEFINE_REFCOUNTED_TYPE(TJobFSSecretary)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
