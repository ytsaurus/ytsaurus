#pragma once

#include <yt/yt/server/node/exec_node/volume.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class THostTmpfsVolume
    : public IVolume
{
public:
    THostTmpfsVolume(
        NProfiling::TTagSet tagSet,
        const std::string& path,
        IInvokerPtr invoker,
        bool detachUnmount);

    ~THostTmpfsVolume() override;

    bool IsCached() const final;

    TFuture<void> Link(
        TGuid tag,
        const std::string& target) override final;

    TFuture<void> Unlink() override final;

    TFuture<void> Remove() override final;

    const TVolumeId& GetId() const override final;

    const std::string& GetPath() const override final;

private:
    const NProfiling::TTagSet TagSet_;
    const std::string Path_;
    const TVolumeId VolumeId_;
    const IInvokerPtr Invoker_;
    const bool DetachUnmount_ = false;
    TFuture<void> RemoveFuture_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
