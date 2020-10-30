#pragma once

#include "public.h"

#include <yt/client/tablet_client/public.h>

#include <yt/client/job_tracker_client/public.h>

#include <yt/client/hydra/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TBuildSnapshotOptions
{
    //! Refers either to masters or to tablet cells.
    //! If null then the primary one is assumed.
    NHydra::TCellId CellId;
    bool SetReadOnly = false;
    bool WaitForSnapshotCompletion = true;
};

struct TBuildMasterSnapshotsOptions
{
    bool SetReadOnly = false;
    bool WaitForSnapshotCompletion = true;
    bool Retry = true;
};

struct TSwitchLeaderOptions
{ };

struct TGCCollectOptions
{
    //! Refers to master cell.
    //! If null then the primary one is assumed.
    NHydra::TCellId CellId;
};

struct TKillProcessOptions
{
    int ExitCode = 42;
};

struct TWriteCoreDumpOptions
{ };

struct TWriteOperationControllerCoreDumpOptions
{ };

using TCellIdToSnapshotIdMap = THashMap<NHydra::TCellId, int>;

struct IAdmin
    : public virtual TRefCounted
{
    virtual TFuture<int> BuildSnapshot(
        const TBuildSnapshotOptions& options = {}) = 0;

    virtual TFuture<TCellIdToSnapshotIdMap> BuildMasterSnapshots(
        const TBuildMasterSnapshotsOptions& options = {}) = 0;

    virtual TFuture<void> SwitchLeader(
        NHydra::TCellId cellId,
        NHydra::TPeerId newLeaderId,
        const TSwitchLeaderOptions& options = {}) = 0;

    virtual TFuture<void> GCCollect(
        const TGCCollectOptions& options = {}) = 0;

    virtual TFuture<void> KillProcess(
        const TString& address,
        const TKillProcessOptions& options = {}) = 0;

    virtual TFuture<TString> WriteCoreDump(
        const TString& address,
        const TWriteCoreDumpOptions& options = {}) = 0;

    virtual TFuture<TString> WriteOperationControllerCoreDump(
        NJobTrackerClient::TOperationId operationId,
        const TWriteOperationControllerCoreDumpOptions& options = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(IAdmin)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

