#pragma once

#include "public.h"
#include "artifact.h"

#include <yt/yt/server/node/exec_node/volume.pb.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/guid.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using TLayerId = TGuid;
using TVolumeId = TGuid;

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMeta
    : public NProto::TVolumeMeta
{
    TVolumeId Id;
    TString MountPath;
};

////////////////////////////////////////////////////////////////////////////////

struct IVolume
    : public virtual TRefCounted
{
    //! Get unique volume id.
    virtual const TVolumeId& GetId() const = 0;
    //! Get absolute path to volume mount point.
    virtual const std::string& GetPath() const = 0;
    //! Overlayfs stores its upper/work directories in root volume.
    virtual bool IsRootVolume() const = 0;
    //! Link volume mount point to target.
    virtual TFuture<void> Link(
        TGuid tag,
        const TString& target) = 0;
    //! Remove volume and links where it points to.
    virtual TFuture<void> Remove() = 0;
    //! Check if volume is cached.
    virtual bool IsCached() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Used for layer and for volume meta files.
struct TLayerMetaHeader
{
    ui64 Signature = ExpectedSignature;

    //! Version of layer meta format. Update every time layer meta version is updated.
    ui64 Version = ExpectedVersion;

    ui64 MetaChecksum;

    static constexpr ui64 ExpectedSignature = 0xbe17d73ce7ff9ea6ull; // YTLMH001
    static constexpr ui64 ExpectedVersion = 1;
};

////////////////////////////////////////////////////////////////////////////////

struct TLayerMeta
    : public NProto::TLayerMeta
{
    std::string Path;
    TLayerId Id;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLayerLocation)
DECLARE_REFCOUNTED_CLASS(TLayer)

class TLayer
    : public TAsyncCacheValueBase<TArtifactKey, TLayer>
{
public:
    TLayer(const TLayerMeta& layerMeta, const TArtifactKey& artifactKey, const TLayerLocationPtr& layerLocation);

    ~TLayer();

    const TString& GetCypressPath() const;

    const std::string& GetPath() const;

    i64 GetSize() const;

    const TLayerMeta& GetMeta() const;

    void IncreaseHitCount();

    int GetHitCount() const;

    void SetLayerRemovalNotNeeded();

private:
    const TLayerMeta LayerMeta_;
    const TLayerLocationPtr Location_;
    std::atomic<int> HitCount_;

    // If the slot manager is disabled, the layers that are currently in the layer cache
    // do not need to be removed from the porto. If you delete them, firstly, in this
    // case they will need to be re-imported into the porto, and secondly, then there
    // may be a problem when inserting the same layers when starting a new volume manager.
    // Namely, a layer object that is already in the new cache may be deleted from the old cache,
    // in which case the layer object in the new cache will be corrupted.
    bool IsLayerRemovalNeeded_ = true;
};

////////////////////////////////////////////////////////////////////////////////

class TOverlayData
{
public:
    TOverlayData() = default;

    explicit TOverlayData(TLayerPtr layer);

    explicit TOverlayData(IVolumePtr volume);

    const std::string& GetPath() const;

    bool IsLayer() const;

    const TLayerPtr& GetLayer() const;

    bool IsVolume() const;

    const IVolumePtr& GetVolume() const;

    TFuture<void> Remove();

private:
    std::variant<TLayerPtr, IVolumePtr> Variant_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSimpleTmpfsVolume)

class TSimpleTmpfsVolume
    : public IVolume
{
public:
    TSimpleTmpfsVolume(
        NProfiling::TTagSet tagSet,
        const std::string& path,
        IInvokerPtr invoker,
        bool detachUnmount);

    ~TSimpleTmpfsVolume() override;

    bool IsCached() const final;

    TFuture<void> Link(
        TGuid tag,
        const TString& target) override final;

    TFuture<void> Remove() override final;

    const TVolumeId& GetId() const override final;

    const std::string& GetPath() const override final;

    bool IsRootVolume() const override final;

private:
    const NProfiling::TTagSet TagSet_;
    const std::string Path_;
    const TVolumeId VolumeId_;
    const IInvokerPtr Invoker_;
    const bool DetachUnmount_;
    TFuture<void> RemoveFuture_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
