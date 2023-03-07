#pragma once

#include "public.h"

#include <yt/client/tablet_client/public.h>

#include <yt/client/job_tracker_client/public.h>

#include <yt/client/election/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TBuildSnapshotOptions
{
    //! Refers either to masters or to tablet cells.
    //! If null then the primary one is assumed.
    NElection::TCellId CellId;
    bool SetReadOnly = false;
    bool WaitForSnapshotCompletion = true;
};

struct TBuildMasterSnapshotsOptions
{
    bool SetReadOnly = false;
    bool WaitForSnapshotCompletion = true;
    bool Retry = true;
};

struct TGCCollectOptions
{
    //! Refers to master cell.
    //! If null then the primary one is assumed.
    NElection::TCellId CellId;
};

struct TKillProcessOptions
{
    int ExitCode = 42;
};

struct TWriteCoreDumpOptions
{ };

using TCellIdToSnapshotIdMap = THashMap<NElection::TCellId, int>;

struct IAdmin
    : public virtual TRefCounted
{
    virtual TFuture<int> BuildSnapshot(
        const TBuildSnapshotOptions& options = TBuildSnapshotOptions()) = 0;

    virtual TFuture<TCellIdToSnapshotIdMap> BuildMasterSnapshots(
        const TBuildMasterSnapshotsOptions& options = TBuildMasterSnapshotsOptions()) = 0;

    virtual TFuture<void> GCCollect(
        const TGCCollectOptions& options = TGCCollectOptions()) = 0;

    virtual TFuture<void> KillProcess(
        const TString& address,
        const TKillProcessOptions& options = TKillProcessOptions()) = 0;

    virtual TFuture<TString> WriteCoreDump(
        const TString& address,
        const TWriteCoreDumpOptions& options = TWriteCoreDumpOptions()) = 0;

    virtual TFuture<TString> WriteOperationControllerCoreDump(
        NJobTrackerClient::TOperationId operationId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAdmin)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

