#pragma once

#include "artifact_description.h"
#include "helpers.h"
#include "preparation_options.h"
#include "private.h"
#include "public.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <optional>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////


struct TJobFSDescription
{
    std::vector<TArtifactDescription> Artifacts;
    THashMap<TString, int> UserArtifactNameToIndex;
    std::vector<TArtifactKey> RootVolumeLayerArtifactKeys;
    std::vector<TArtifactKey> GpuCheckVolumeLayerArtifactKeys;
    std::optional<TString> DockerImage;
    std::optional<int> RootVolumeDiskSpace;
    std::optional<int64_t> RootVolumeInodeLimit;
    std::vector<TBaseVolumeParamsPtr> NonRootVolumeParams;
    std::vector<NScheduler::TVolumeMountPtr> JobVolumeMounts;
    std::optional<TSandboxNbdRootVolumeData> SandboxNbdRootVolumeData;
};

////////////////////////////////////////////////////////////////////////////////

//! Stores job artifacts and layer keys (does not prepare or download them).
class TJobFSSecretary
    : public TRefCounted
{
public:
    TJobFSSecretary(
        IBootstrap* bootstrap,
        NLogging::TLogger logger);

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

    const std::vector<TVolumeResultPtr>& GetNonRootVolumes() const;
    std::vector<TVolumeResultPtr> ReleaseNonRootVolumes();
    void SetNonRootVolumes(std::vector<TVolumeResultPtr> volumes);

    size_t GetTmpfsVolumeCount() const;

    const std::optional<TVirtualSandboxData>& GetVirtualSandboxData() const;
    void SetVirtualSandboxReader(NNbd::IImageReaderPtr reader);

    const std::optional<int>& GetRootVolumeDiskSpace() const;
    const std::optional<int64_t>& GetRootVolumeInodeLimit() const;

    const std::vector<TBaseVolumeParamsPtr>& GetNonRootVolumeParams() const;

    const std::vector<NScheduler::TVolumeMountPtr>& GetJobVolumeMounts() const;

    const TArtifactDescription& GetUserArtifact(const TString& name) const;

    //! Returns artifact descriptions that need to be cached
    //! (excludes artifacts that bypass cache or are accessed via virtual sandbox).
    std::vector<TArtifactDescription> GetArtifactsToCache() const;

    //! Sets cached artifact pointers. The size must match GetArtifactsToCache().
    //! Uses the same filtering logic to find the right slots in Artifacts_.
    void SetCachedArtifacts(std::vector<TArtifactPtr> artifacts);

    void ReleaseArtifacts();

private:
    TJobId JobId_;
    IBootstrap* const Bootstrap_;
    const NLogging::TLogger BaseLogger_;
    NLogging::TLogger Logger;
    bool RootVolumeDiskQuotaEnabled_ = false;

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
    std::vector<TVolumeResultPtr> NonRootVolumes_;
    std::optional<TVirtualSandboxData> VirtualSandboxData_;
    // COMPAT(krasovav)
    std::optional<int> RootVolumeDiskSpace_;
    // COMPAT(krasovav)
    std::optional<int64_t> RootVolumeInodeLimit_;
    std::vector<TBaseVolumeParamsPtr> NonRootVolumeParams_;
    std::vector<NScheduler::TVolumeMountPtr> JobVolumeMounts_;
    bool HasVirtualSandboxArtifacts_ = false;
    bool ArtifactsCached_ = false;

    void ConfigureUserArtifacts(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureLayerArtifacts(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureDockerImage(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureUdfArtifacts(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TJobSpecExt& jobSpecExt);
    void ConfigureNbdDeviceIds();
    void ConfigureVolumes(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec, int userId);
    void VerifyDescriptionMatchesApplied(const TJobFSDescription& current) const;
    void ApplyDescription(TNonNullPtr<TJobFSDescription> description);
    void CheckConfiguration(bool hasNbdServer) const;

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
