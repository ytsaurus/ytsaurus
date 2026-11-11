#pragma once

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <string>
#include <vector>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJobFilesystem
{
public:
    TJobFilesystem(IInvokerPtr invoker, bool privileged);

    bool UsesPrivilegedAccess() const;

    TFuture<void> RemoveDirectory(const std::string& path, TRefCountedPtr owner) const;
    TFuture<i64> GetDirectoriesSize(std::vector<std::string> paths, TRefCountedPtr owner) const;

private:
    const IInvokerPtr Invoker_;
    const bool Privileged_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
