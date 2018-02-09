#pragma once

#include <yt/core/misc/public.h>

namespace NYT {
namespace NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TBind
{
    TString SourcePath;
    TString TargetPath;
    bool IsReadOnly;
};

struct TRootFS
{
    TString RootPath;
    bool IsRootReadOnly;
    std::vector<TBind> Binds;
};

DECLARE_REFCOUNTED_STRUCT(IContainerManager)
DECLARE_REFCOUNTED_STRUCT(IInstance)
DECLARE_REFCOUNTED_STRUCT(IPortoExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespace NYT
