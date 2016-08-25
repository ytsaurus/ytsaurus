#pragma once

#include "public.h"
#include "connection.h"

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NApi {

///////////////////////////////////////////////////////////////////////////////

struct TBuildSnapshotOptions
{
    //! Refers either to masters or to tablet cells.
    //! If null then the primary master is assumed.
    NElection::TCellId CellId;
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

