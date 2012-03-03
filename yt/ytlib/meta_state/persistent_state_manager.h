#pragma once

#include "config.h"
#include "meta_state_manager.h"
#include "change_log_downloader.h"
#include "snapshot_downloader.h"
#include "follower_pinger.h"
#include "change_committer.h"

#include <ytlib/election/election_manager.h>

#include <ytlib/rpc/server.h>
#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TMetaStateManager.
struct TPersistentStateManagerConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TPersistentStateManagerConfig> TPtr;

    //! A path where changelogs are stored.
    Stroka LogPath;

    //! A path where snapshots are stored.
    Stroka SnapshotPath;

    //! Snapshotting period (measured in number of changes).
    /*!
     *  This is also an upper limit for the number of records in a changelog.
     *  
     *  The limit may be violated if the server is under heavy load and
     *  a new snapshot generation request is issued when the previous one is still in progress.
     *  This situation is considered abnormal and a warning is reported.
     *  
     *  A special value of -1 means that snapshot creation is switched off.
     */
    i32 MaxChangesBetweenSnapshots;

    //! Maximum time a follower waits for "Sync" request from the leader.
    TDuration SyncTimeout;

    //! Default timeout for RPC requests.
    TDuration RpcTimeout;

    // TODO: refactor
    TCellConfig::TPtr Cell;

    NElection::TElectionManager::TConfig::TPtr Election;

    TChangeLogDownloader::TConfig::TPtr ChangeLogDownloader;

    TSnapshotDownloader::TConfig::TPtr SnapshotDownloader;

    TFollowerPinger::TConfig::TPtr FollowerPinger;

    TFollowerTracker::TConfig::TPtr FollowerTracker;

    TLeaderCommitter::TConfig::TPtr LeaderCommitter;

    TPersistentStateManagerConfig()
    {
        Register("log_path", LogPath)
            .NonEmpty();
        Register("snapshot_path", SnapshotPath)
            .NonEmpty();
        Register("max_changes_between_snapshots", MaxChangesBetweenSnapshots)
            .Default(-1)
            .GreaterThanOrEqual(-1);
        Register("sync_timeout", SyncTimeout)
            .Default(TDuration::MilliSeconds(5000));
        Register("rpc_timeout", RpcTimeout)
            .Default(TDuration::MilliSeconds(3000));
        Register("cell", Cell)
            .DefaultNew();
        Register("election", Election)
            .DefaultNew();
        Register("change_log_downloader", ChangeLogDownloader)
            .DefaultNew();
        Register("snapshot_downloader", SnapshotDownloader)
            .DefaultNew();
        Register("follower_pinger", FollowerPinger)
            .DefaultNew();
        Register("follower_tracker", FollowerTracker)
            .DefaultNew();
        Register("leader_committer", LeaderCommitter)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Creates the manager and also registers its RPC service at #server.
IMetaStateManager::TPtr CreatePersistentStateManager(
    TPersistentStateManagerConfig* config,
    IInvoker* controlInvoker,
    IInvoker* stateInvoker,
    IMetaState* metaState,
    NRpc::IServer* server);

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
