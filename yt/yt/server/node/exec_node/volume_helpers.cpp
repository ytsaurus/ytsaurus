#include "volume_helpers.h"

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkNbdVolumeSpec& volumeSpec, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{Kind: Chunk, MediumIndex: %v, DataNodeAddress: %v, MinDataNodeCount: %v, MaxDataNodeCount: %v, "
        "DataNodeRpcTimeout: %v, DataNodeNbdServiceRpcTimeout: %v, "
        "DataNodeNbdServiceMakeTimeout: %v, MasterRpcTimeout: %v, MultiplexingParallelism: %v}",
        volumeSpec.MediumIndex,
        volumeSpec.DataNodeAddress,
        volumeSpec.MinDataNodeCount,
        volumeSpec.MaxDataNodeCount,
        volumeSpec.DataNodeRpcTimeout,
        volumeSpec.DataNodeNbdServiceRpcTimeout,
        volumeSpec.DataNodeNbdServiceMakeTimeout,
        volumeSpec.MasterRpcTimeout,
        volumeSpec.MultiplexingParallelism);
}

void FormatValue(TStringBuilderBase* builder, const TSandboxNbdRootVolumeSpec& volumeSpec, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{DeviceSize: %v, FilesystemType: %v, BackendSpec: %v}",
        volumeSpec.DeviceSize,
        volumeSpec.FilesystemType,
        volumeSpec.BackendSpec);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TBaseVolumeParamsPtr& params, TStringBuf /*spec*/)
{
    if (!params) {
        return;
    }
    builder->AppendFormat("{");
    params->Format(builder);
    builder->AppendString("}");
}

bool TBaseVolumeParams::TLayerArtifactKeysStorage::operator==(const TLayerArtifactKeysStorage& other) const
{
    const auto regularCount =
        std::ssize(MergedArtifactKeys_) - VolatileArtifactKeyCount_;
    const auto otherRegularCount =
        std::ssize(other.MergedArtifactKeys_) - other.VolatileArtifactKeyCount_;

    if (regularCount != otherRegularCount) {
        return false;
    }

    return std::equal(
        MergedArtifactKeys_.begin() + VolatileArtifactKeyCount_,
        MergedArtifactKeys_.end(),
        other.MergedArtifactKeys_.begin() + other.VolatileArtifactKeyCount_);
}

std::span<TArtifactKey> TBaseVolumeParams::TLayerArtifactKeysStorage::GetAll()
{
    return MergedArtifactKeys_;
}

std::span<const TArtifactKey> TBaseVolumeParams::TLayerArtifactKeysStorage::GetAll() const
{
    return MergedArtifactKeys_;
}

void TBaseVolumeParams::TLayerArtifactKeysStorage::AddRegularArtifactKeys(std::vector<TArtifactKey> regularArtifactKeys)
{
    YT_VERIFY(VolatileArtifactKeyCount_ == std::ssize(MergedArtifactKeys_));

    MergedArtifactKeys_.insert(
        MergedArtifactKeys_.end(),
        std::make_move_iterator(regularArtifactKeys.begin()),
        std::make_move_iterator(regularArtifactKeys.end()));
}

void TBaseVolumeParams::TLayerArtifactKeysStorage::SetVolatileArtifactKeys(std::vector<TArtifactKey> volatileArtifactKeys)
{
    std::vector<TArtifactKey> mergedArtifactKeys;
    mergedArtifactKeys.reserve(
        volatileArtifactKeys.size() +
        MergedArtifactKeys_.size() -
        VolatileArtifactKeyCount_);

    std::move(
        volatileArtifactKeys.begin(),
        volatileArtifactKeys.end(),
        std::back_inserter(mergedArtifactKeys));

    std::move(
        MergedArtifactKeys_.begin() + VolatileArtifactKeyCount_,
        MergedArtifactKeys_.end(),
        std::back_inserter(mergedArtifactKeys));

    VolatileArtifactKeyCount_ = std::ssize(volatileArtifactKeys);
    MergedArtifactKeys_ = std::move(mergedArtifactKeys);
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
        case EVolumeType::Nbd:
            return static_cast<const TNbdDiskVolumeParams&>(*this) == static_cast<const TNbdDiskVolumeParams&>(other);
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

TNbdDiskVolumeParams::TNbdDiskVolumeParams(
    std::string volumeId,
    int userId,
    TSandboxNbdRootVolumeSpec sandboxNbdRootVolumeSpec)
    : TBaseVolumeParams(std::move(volumeId), EVolumeType::Nbd, userId)
    , SandboxNbdRootVolumeSpec(std::move(sandboxNbdRootVolumeSpec))
{ }

void TNbdDiskVolumeParams::Format(TStringBuilderBase* builder) const
{
    TBaseVolumeParams::Format(builder);
    builder->AppendFormat(", SandboxNbdRootVolumeSpec: %v", SandboxNbdRootVolumeSpec);
}

bool TNbdDiskVolumeParams::operator==(const TNbdDiskVolumeParams& other) const
{
    return SandboxNbdRootVolumeSpec == other.SandboxNbdRootVolumeSpec;
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

void Serialize(const TVolumeMount& volumeMount, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("volume_id").Value(volumeMount.VolumeId)
            .Item("mount_path").Value(volumeMount.MountPath.Path())
            .Item("read_only").Value(volumeMount.ReadOnly)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
