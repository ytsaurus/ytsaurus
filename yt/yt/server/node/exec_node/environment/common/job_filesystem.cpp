#include "job_filesystem.h"

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NExecNode {

using namespace NTools;

////////////////////////////////////////////////////////////////////////////////

TJobFilesystem::TJobFilesystem(IInvokerPtr invoker, bool privileged)
    : Invoker_(std::move(invoker))
    , Privileged_(privileged)
{ }

bool TJobFilesystem::UsesPrivilegedAccess() const
{
    return Privileged_;
}

TFuture<void> TJobFilesystem::RemoveDirectory(const std::string& path, TRefCountedPtr owner) const
{
    if (!Privileged_) {
        try {
            NFS::RemoveRecursive(path);
            return OKFuture;
        } catch (const std::exception& ex) {
            return MakeFuture<void>(TError(ex));
        }
    }

    return BIND([path, owner = std::move(owner)] {
        RunTool<TRemoveDirAsRootTool>(path);
    })
        .AsyncVia(Invoker_)
        .Run();
}

TFuture<i64> TJobFilesystem::GetDirectoriesSize(std::vector<std::string> paths, TRefCountedPtr owner) const
{
    if (!Privileged_) {
        try {
            return MakeFuture(NFS::GetDirectoriesSize(paths, /*ignoreUnavailableFiles*/ true, /*deduplicateByINodes*/ true));
        } catch (const std::exception& ex) {
            return MakeFuture<i64>(TError(ex));
        }
    }

    auto config = New<TGetDirectorySizesAsRootConfig>();
    config->IgnoreUnavailableFiles = true;
    config->DeduplicateByINodes = true;
    config->CheckDeviceId = true;
    config->Paths = std::move(paths);

    return BIND([config = std::move(config), owner = std::move(owner)] {
        return RunTool<TGetTotalDirectoriesSizeAsRootTool>(config);
    })
        .AsyncVia(Invoker_)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
