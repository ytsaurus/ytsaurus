#pragma once

#include <yt/yt/server/node/exec_node/volume.h>

#include <yt/yt/server/node/exec_node/environment/porto/volume.pb.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

using TLayerId = TGuid;

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMeta
    : public NProto::TVolumeMeta
{
    TVolumeId Id;
    std::string MountPath;
    //! Path to custom porto place.
    std::optional<std::string> PortoPlacePath;
};

////////////////////////////////////////////////////////////////////////////////

//! Used for layer and for volume meta files.
struct TLayerMetaHeader
{
    ui64 Signature = ExpectedSignature;

    //! Version of layer meta format. Update every time layer meta version is updated.
    ui64 Version = ExpectedVersion;

    ui64 MetaChecksum = 0;

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
