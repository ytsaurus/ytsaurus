#include "tmpfs_volume.h"

#include <yt/yt/server/node/exec_node/private.h>
#include <yt/yt/server/node/exec_node/volume_counters.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/actions/bind.h>

#include <util/digest/city.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

THostTmpfsVolume::THostTmpfsVolume(
    TTagSet tagSet,
    const std::string& path,
    IInvokerPtr invoker,
    bool detachUnmount)
    : TagSet_(std::move(tagSet))
    , Path_(path)
    , VolumeId_(
        [&path] {
            auto [low, high] = CityHash128(path.c_str(), path.size());
            return TGuid(low, high);
        }())
    , Invoker_(std::move(invoker))
    , DetachUnmount_(detachUnmount)
{ }

THostTmpfsVolume::~THostTmpfsVolume()
{
    YT_UNUSED_FUTURE(Remove());
}

bool THostTmpfsVolume::IsCached() const
{
    return false;
}

TFuture<void> THostTmpfsVolume::Link(
    TGuid /*tag*/,
    const std::string& /*target*/)
{
    // Simple volume is created inside sandbox, so we don't need to link it.
    YT_UNIMPLEMENTED("Link is not implemented for SimpleTmpfsVolume");
}

TFuture<void> THostTmpfsVolume::Unlink()
{
    // Simple volume is created inside sandbox, so we don't need to unlink it.
    return OKFuture;
}

TFuture<void> THostTmpfsVolume::Remove()
{
    if (RemoveFuture_) {
        return RemoveFuture_;
    }

    TEventTimerGuard volumeRemoveTimeGuard(TVolumeProfilerCounters::Get()->GetTimer(TagSet_, "/remove_time"));

    const auto volumeType = EVolumeType::Tmpfs;
    const auto& volumeId = VolumeId_;
    const auto& volumePath = Path_;

    auto Logger = ExecNodeLogger()
        .WithTag("VolumeType", volumeType)
        .WithTag("VolumeId", volumeId)
        .WithTag("VolumePath", volumePath);

    RemoveFuture_ = BIND(
        [
            tagSet = TagSet_,
            Logger,
            this,
            this_ = MakeStrong(this)
        ] {
            try {
                RunTool<TRemoveDirContentAsRootTool>(Path_);

                auto config = New<TUmountConfig>();
                config->Path = Path_;
                config->Detach = DetachUnmount_;
                RunTool<TUmountAsRootTool>(config);

                TVolumeProfilerCounters::Get()->GetGauge(tagSet, "/count")
                    .Update(VolumeCounters().Decrement(tagSet));
                TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/removed").Increment(1);
            } catch (const std::exception& ex) {
                TVolumeProfilerCounters::Get()->GetCounter(tagSet, "/remove_errors").Increment(1);

                YT_TLOG_ERROR("Failed to remove volume")
                    .With(ex);

                THROW_ERROR_EXCEPTION("Failed to remove volume")
                    .With(ex);
            }
        })
        .AsyncVia(Invoker_)
        .Run()
        .ToUncancelable();

    return RemoveFuture_;
}

const TVolumeId& THostTmpfsVolume::GetId() const
{
    return VolumeId_;
}

const std::string& THostTmpfsVolume::GetPath() const
{
    return Path_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
