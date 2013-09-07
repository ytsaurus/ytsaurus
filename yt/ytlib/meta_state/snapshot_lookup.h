#pragma once

#include "public.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/concurrency/parallel_awaiter.h>
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

    int GetLatestSnapshotId(int maxSnapshotId);

private:
    typedef TMetaStateManagerProxy TProxy;

    TPersistentStateManagerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TSpinLock SpinLock;
    int CurrentSnapshotId;
    TPromise<int> Promise;

    void OnLookupSnapshotResponse(
        TPeerId peerId,
        TProxy::TRspLookupSnapshotPtr response);
    void OnLookupSnapshotComplete();
    
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
