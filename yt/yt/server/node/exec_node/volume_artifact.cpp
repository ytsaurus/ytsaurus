#include "volume_artifact.h"

#include "artifact.h"
#include "artifact_cache.h"

#include <yt/yt/core/actions/bind.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactAdapter
    : public IVolumeArtifact
{
public:
    explicit TVolumeArtifactAdapter(TArtifactPtr artifact)
        : Artifact_(std::move(artifact))
    { }

    const std::string& GetFileName() const override
    {
        return Artifact_->GetFileName();
    }

private:
    const TArtifactPtr Artifact_;
};

////////////////////////////////////////////////////////////////////////////////

class TVolumeArtifactCacheAdapter
    : public IVolumeArtifactCache
{
public:
    explicit TVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache)
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
    const TArtifactCachePtr ArtifactCache_;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeArtifactCachePtr CreateVolumeArtifactCacheAdapter(TArtifactCachePtr artifactCache)
{
    return New<TVolumeArtifactCacheAdapter>(std::move(artifactCache));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
