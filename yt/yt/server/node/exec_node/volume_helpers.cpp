#include "volume_helpers.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TBaseVolumeParamsPtr& params, TStringBuf /*spec*/)
{
    builder->AppendFormat("{");
    params->Format(builder);
    builder->AppendString("}");
}

TBaseVolumeParams::TBaseVolumeParams(std::string volumeId, EVolumeType volumeType, int userId)
    : VolumeId(std::move(volumeId))
    , VolumeType(volumeType)
    , UserId(userId)
{ }

bool TBaseVolumeParams::operator==(const TBaseVolumeParams& other) const
{
    // First compare all base class members.
    if (VolumeId != other.VolumeId ||
        VolumeType != other.VolumeType ||
        UserId != other.UserId ||
        Size != other.Size ||
        LayerArtifactKeys != other.LayerArtifactKeys ||
        AllowReusing != other.AllowReusing)
    {
        return false;
    }

    // Then dispatch to derived class to compare derived-specific members.
    switch (VolumeType) {
        case EVolumeType::Tmpfs:
            return static_cast<const TTmpfsVolumeParams&>(*this) == static_cast<const TTmpfsVolumeParams&>(other);
        case EVolumeType::LocalDisk:
            return static_cast<const TLocalDiskVolumeParams&>(*this) == static_cast<const TLocalDiskVolumeParams&>(other);
        default:
            YT_ABORT();
    }
}

void TBaseVolumeParams::Format(TStringBuilderBase* builder) const
{
    builder->AppendFormat("UserId: %v, VolumeId: %v, Size: %v, AllowReusing: %v", UserId, VolumeId, Size, AllowReusing);
}

////////////////////////////////////////////////////////////////////////////////

TTmpfsVolumeParams::TTmpfsVolumeParams(std::string volumeId, int userId)
    : TBaseVolumeParams(std::move(volumeId), EVolumeType::Tmpfs, userId)
{ }

void TTmpfsVolumeParams::Format(TStringBuilderBase* builder) const
{
    TBaseVolumeParams::Format(builder);
    builder->AppendFormat(", TmpfsIndex: %v", Index);
}

bool TTmpfsVolumeParams::operator==(const TTmpfsVolumeParams& other) const
{
    // NB: We only compare derived class members here.
    // Base class members (VolumeId, Size, LayerArtifactKeys, AllowReusing) are compared
    // by TBaseVolumeParams::operator== before dispatching to this method.
    return Index == other.Index;
}

////////////////////////////////////////////////////////////////////////////////

TLocalDiskVolumeParams::TLocalDiskVolumeParams(std::string volumeId, int userId)
    : TBaseVolumeParams(std::move(volumeId), EVolumeType::LocalDisk, userId)
{ }

void TLocalDiskVolumeParams::Format(TStringBuilderBase* builder) const
{
    TBaseVolumeParams::Format(builder);
    builder->AppendFormat(", InodeLimit: %v", InodeLimit);
}

bool TLocalDiskVolumeParams::operator==(const TLocalDiskVolumeParams& other) const
{
    // NB: We only compare derived class members here.
    // Base class members (VolumeId, Size, LayerArtifactKeys, AllowReusing) are compared
    // by TBaseVolumeParams::operator== before dispatching to this method.
    return InodeLimit == other.InodeLimit;
}

////////////////////////////////////////////////////////////////////////////////

TVolumeResult::TVolumeResult(std::string volumeId, EVolumeType volumeType, IVolumePtr&& volume)
    : VolumeId(std::move(volumeId))
    , VolumeType(volumeType)
    , Volume(std::move(volume))
{ }

////////////////////////////////////////////////////////////////////////////////

TTmpfsVolumeResult::TTmpfsVolumeResult(std::string volumeId, EVolumeType volumeType, IVolumePtr&& volume, int index)
    : TVolumeResult(std::move(volumeId), volumeType, std::move(volume))
    , Index(index)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
