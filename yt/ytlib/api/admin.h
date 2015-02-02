#pragma once

#include "public.h"
#include "connection.h"

#include <core/actions/future.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TBuildSnapshotOptions
{
    bool SetReadOnly = false;
};

struct TGCCollectOptions
{ };

struct IAdmin
    : public virtual TRefCounted
{
    virtual TFuture<int> BuildSnapshot(
        const TBuildSnapshotOptions& options = TBuildSnapshotOptions()) = 0;

    virtual TFuture<void> GCCollect(
        const TGCCollectOptions& options = TGCCollectOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAdmin)

///////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

