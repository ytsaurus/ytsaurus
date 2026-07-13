#pragma once

#include "artifact_description.h"
#include "helpers.h"
#include "preparation_options.h"
#include "private.h"
#include "public.h"
#include "volume.h"

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

#include <optional>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! Prepared overlay layer data, indexed by artifact key.
struct TPreparedLayers
{
    THashMap<TArtifactKey, TOverlayData> ArtifactKeyToOverlayData;
};

struct TJobFSDescription
    : public TRefCounted
{
    std::vector<TArtifactDescription> Artifacts;
    THashMap<std::string, int> UserArtifactNameToIndex;
    std::vector<TArtifactKey> RootVolumeLayerArtifactKeys;
    std::vector<TArtifactKey> GpuCheckVolumeLayerArtifactKeys;
    std::optional<std::string> DockerImage;
    std::optional<i64> RootVolumeDiskSpace;
    std::optional<i64> RootVolumeInodeLimit;
    bool RootVolumeAllowReusing = false;
    std::vector<TBaseVolumeParamsPtr> NonRootVolumeParams;
    std::vector<TVolumeMountPtr> JobVolumeMounts;
    THashMap<std::string, std::vector<TVolumeMountPtr>> SidecarsVolumeMounts;
    std::optional<TSandboxNbdRootVolumeData> SandboxNbdRootVolumeData;
};
DEFINE_REFCOUNTED_TYPE(TJobFSDescription)

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

    const std::vector<TArtifactDescription>& GetArtifactDescriptors() const;

    const std::vector<TArtifactKey>& GetRootVolumeLayerArtifactKeys() const;
    const std::vector<TArtifactKey>& GetGpuCheckVolumeLayerArtifactKeys() const;

    const std::optional<std::string>& GetDockerImage() const;
    void SetDockerImage(std::optional<std::string> image);

    const std::optional<std::string>& GetDockerImageId() const;
    void SetDockerImageId(std::optional<std::string> imageId);

    const IVolumePtr& GetRootVolume() const;
    void SetRootVolume(IVolumePtr volume);
    IVolumePtr ReleaseRootVolume();

    const IVolumePtr& GetGpuCheckVolume() const;
    void SetGpuCheckVolume(IVolumePtr volume);
    IVolumePtr ReleaseGpuCheckVolume();

    bool IsRootVolumeDiskQuotaEnabled() const;

    const THashSet<std::string>& GetNbdDeviceIds() const;
    THashSet<std::string> ReleaseNbdDeviceIds();

    const std::optional<TSandboxNbdRootVolumeData>& GetSandboxNbdRootVolumeData() const;

    const THashMap<std::string, TVolumeResultPtr>& GetNonRootVolumes() const;
    THashMap<std::string, TVolumeResultPtr> ReleaseNonReusableNonRootVolumes();
    void SetNonRootVolumes(std::vector<TVolumeResultPtr> volumes);

    //! Returns volume params that need preparation (excludes already prepared reusable volumes).
    std::vector<TBaseVolumeParamsPtr> GetNonRootVolumesToPrepare() const;

    size_t GetTmpfsVolumeCount() const;

    const std::optional<TVirtualSandboxData>& GetVirtualSandboxData() const;
    void SetVirtualSandboxReader(NNbd::NImage::IImageReaderPtr reader);

    const std::optional<i64>& GetRootVolumeDiskSpace() const;
    const std::optional<i64>& GetRootVolumeInodeLimit() const;
    bool IsRootVolumeReusable() const;
    IVolumePtr ReleaseRootVolumeIfNeeded();
    std::vector<IVolumePtr> ReleaseVolumes();

    const std::vector<TBaseVolumeParamsPtr>& GetNonRootVolumeParams() const;

    const std::vector<TVolumeMountPtr>& GetJobVolumeMounts() const;

    const THashMap<std::string, std::vector<TVolumeMountPtr>>& GetSidecarsVolumeMounts() const;

    const TArtifactPtr& GetArtifactByName(const std::string& name) const;

    const TArtifactDescription& GetUserArtifactDescriptor(const std::string& name) const;

    //! Adds prepared overlay layers to the allocation-scoped cache.
    //! Crashes if any of the keys is already present.
    void AddPreparedLayers(TPreparedLayers layers);

    bool HasPreparedLayer(const TArtifactKey& key) const;

    std::vector<TOverlayData> GetPreparedRootVolumeOverlayData() const;
    std::vector<TOverlayData> GetPreparedGpuCheckVolumeOverlayData() const;
    std::vector<TOverlayData> GetPreparedNonRootVolumeOverlayData(const TBaseVolumeParams& params) const;

    void ReleasePreparedLayers();

    //! Returns artifact descriptions that need to be cached
    //! (excludes artifacts that bypass cache or are accessed via virtual sandbox).
    std::vector<TArtifactDescription> GetArtifactsToCache() const;

    //! Sets cached artifact pointers. The size must match GetArtifactsToCache().
    //! Uses the same filtering logic to find the right slots in Artifacts.
    void SetCachedArtifacts(std::vector<TArtifactPtr> artifacts);

    void ReleaseArtifacts();

private:
    TJobId JobId_;
    IBootstrap* const Bootstrap_;
    const NLogging::TLogger BaseLogger_;
    NLogging::TLogger Logger;
    bool RootVolumeDiskQuotaEnabled_ = false;

    std::optional<std::string> ActualDockerImage_;
    std::optional<std::string> DockerImageId_;
    IVolumePtr RootVolume_;
    IVolumePtr GpuCheckVolume_;
    std::vector<TArtifactKey> MergedRootVolumeLayerArtifactKeys_;
    std::vector<TArtifactKey> MergedGpuCheckVolumeLayerArtifactKeys_;
    THashSet<std::string> NbdDeviceIds_;
    THashMap<std::string, TVolumeResultPtr> NonRootVolumes_;
    std::optional<TVirtualSandboxData> VirtualSandboxData_;
    THashMap<std::string, TArtifactPtr> NameToPreparedArtifacts_;
    bool HasVirtualSandboxArtifacts_ = false;
    bool ArtifactsCached_ = false;

    TIntrusivePtr<const TJobFSDescription> Description_ = New<const TJobFSDescription>();
    TPreparedLayers PreparedLayers_;

    void ConfigureUserArtifacts(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureLayerArtifacts(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureDockerImage(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void ConfigureUdfArtifacts(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TJobSpecExt& jobSpecExt);
    void ConfigureNbdDeviceIds(TNonNullPtr<TJobFSDescription> description);
    void ConfigureVolumes(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec, int userId);
    void AddGpuToppingLayersIfNeeded(const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void VerifyDescriptionMatchesApplied(const TJobFSDescriptionPtr& current) const;
    void ApplyDescription(TJobFSDescriptionPtr&& description);
    void CheckConfiguration(bool hasNbdServer) const;

    void MarkArtifactsAccessedViaVirtualSandbox(TNonNullPtr<TJobFSDescription> description, const NControllerAgent::NProto::TUserJobSpec* userJobSpec);
    void MarkArtifactsAccessedViaBind(TNonNullPtr<TJobFSDescription> description);
    bool CanBeAccessedViaVirtualSandbox(const TArtifactDescription& artifact, const NControllerAgent::NProto::TUserJobSpec* userJobSpec) const;
    bool CanBeAccessedViaBind(const TArtifactDescription& artifact) const;

    void OnNewJobStarted(TJobId jobId);

    std::vector<TOverlayData> GetPreparedOverlayData(const std::vector<TArtifactKey>& artifactKeys) const;
};

DEFINE_REFCOUNTED_TYPE(TJobFSSecretary)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
