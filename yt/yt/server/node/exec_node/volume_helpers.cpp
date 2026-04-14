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
    if (VolumeType != other.VolumeType) {
        return false;
    }

    switch (VolumeType) {
        case EVolumeType::Tmpfs:
            return static_cast<const TTmpfsVolumeParams&>(*this) == static_cast<const TTmpfsVolumeParams&>(other);
        case EVolumeType::Local:
            return static_cast<const TLocalDiskVolumeParams&>(*this) == static_cast<const TLocalDiskVolumeParams&>(other);
        default:
            YT_ABORT();
    }
}

void TBaseVolumeParams::Format(TStringBuilderBase* builder) const
{
    builder->AppendFormat("UserId: %v, VolumeId: %v, Size: %v", UserId, VolumeId, Size);
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

////////////////////////////////////////////////////////////////////////////////

TLocalDiskVolumeParams::TLocalDiskVolumeParams(std::string volumeId, int userId)
    : TBaseVolumeParams(std::move(volumeId), EVolumeType::Local, userId)
{ }

void TLocalDiskVolumeParams::Format(TStringBuilderBase* builder) const
{
    TBaseVolumeParams::Format(builder);
    builder->AppendFormat(", InodeLimit: %v", InodeLimit);
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
