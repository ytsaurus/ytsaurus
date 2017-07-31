#include "config.h"
#include "mounter.h"
#include "slot_location.h"
#include "private.h"

#include <yt/server/data_node/master_connector.h>

#include <yt/server/misc/disk_health_checker.h>

#include <yt/server/containers/instance.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/proc.h>
#include <yt/core/misc/singleton.h>

#include <yt/core/tools/tools.h>

#include <yt/core/yson/writer.h>

#include <yt/core/ytree/convert.h>

#include <util/system/fs.h>

namespace NYT {
namespace NExecAgent {

using namespace NConcurrency;
using namespace NContainers;
using namespace NTools;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSimpleMounter
    : public IMounter
{
public:
    TSimpleMounter(const IInvokerPtr invoker)
        : MountInvoker_(invoker)
    { }

    virtual std::vector<NFS::TMountPoint> GetMountPoints() const override
    {
        auto asyncResult = BIND(NFS::GetMountPoints)
            .AsyncVia(MountInvoker_)
            .Run("/proc/mounts");
        auto result = WaitFor(asyncResult)
            .ValueOrThrow();
        return result;
    }

    virtual void Mount(TMountTmpfsConfigPtr config) override
    {
        RunTool<TMountTmpfsAsRootTool>(config);
    }

    virtual void Umount(TUmountConfigPtr config) override
    {
        RunTool<TUmountAsRootTool>(config);
    }

private:
    const IInvokerPtr MountInvoker_;
};

////////////////////////////////////////////////////////////////////////////////

class TPortoMounter
    : public IMounter
{
public:
    explicit TPortoMounter(TCallback<IInstancePtr()> instanceProvider)
        : InstanceProvider_(instanceProvider)
    { }

    virtual std::vector<NFS::TMountPoint> GetMountPoints() const override
    {
        if (Container_) {
            return Container_->ListVolumes();
        }
        return {};
    }

    virtual void Mount(TMountTmpfsConfigPtr config) override
    {
        if (!Container_) {
            Container_ = InstanceProvider_();
        }
        Container_->MountTmpfs(
            config->Path,
            config->Size,
            ToString(config->UserId));
    }

    virtual void Umount(TUmountConfigPtr config) override
    {
        if (Container_) {
            Container_->Umount(config->Path);
        }
    }

private:
    const TCallback<IInstancePtr()> InstanceProvider_;

    IInstancePtr Container_;
};

////////////////////////////////////////////////////////////////////////////////

IMounterPtr CreateSimpleMounter(const IInvokerPtr invoker)
{
    return New<TSimpleMounter>(invoker);
}

IMounterPtr CreatePortoMounter(TCallback<IInstancePtr()> instanceProvider)
{
    return New<TPortoMounter>(instanceProvider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT
