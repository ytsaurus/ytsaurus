#include "volume_artifact.h"

#include "artifact.h"
#include "artifact_cache.h"

#include <yt/yt/core/actions/bind.h>

namespace NYT::NExecNode {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IVolumeArtifact)

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactAdapter
    : public IVolumeArtifact
{
public:
    TVolumeArtifactAdapter(TArtifactPtr artifact)
        : Artifact_(std::move(artifact))
    { }

    const std::string& GetFileName() const override
    {
        return Artifact_->GetFileName();
    }

private:
    TArtifactPtr Artifact_;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(IVolumeArtifactCache)

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactCacheAdapter
    : public IVolumeArtifactCache
{
public:
    TVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache)
        : ArtifactCache_(artifactCache)
    { }

    TFuture<IVolumeArtifactPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions) override
    {
        auto artifact = ArtifactCache_->DownloadArtifact(key, artifactDownloadOptions);
        return artifact.Apply(BIND([] (TArtifactPtr artifact) {
            return IVolumeArtifactPtr(New<TVolumeArtifactAdapter>(artifact));
        }));
    }

private:
    TArtifactCachePtr ArtifactCache_;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeArtifactCachePtr CreateVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache)
{
    return New<TVolumeArtifactCacheAdapter>(std::move(artifactCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
