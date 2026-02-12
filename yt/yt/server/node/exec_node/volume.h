#pragma once

#include "public.h"

#include <yt/yt/server/node/exec_node/volume.pb.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/guid.h>

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

DEFINE_REFCOUNTED_TYPE(IVolume)

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

} // namespace NYT::NExecNode
