#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/actions/future.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotLookup
    : private TNonCopyable
{
public:
    TSnapshotLookup(
        TPersistentStateManagerConfigPtr config,
        NElection::TCellManagerPtr cellManager);

    i32 LookupLatestSnapshot(i32 maxSnapshotId);

private:
    typedef TMetaStateManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

    TPersistentStateManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    i32 CurrentSnapshotId;
    TPromise<i32> Promise;

    void OnLookupSnapshotResponse(
        TPeerId peerId,
        TProxy::TRspLookupSnapshotPtr response);
    void OnLookupSnapshotComplete();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
