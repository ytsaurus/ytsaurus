#pragma once

#include "../misc/common.h"
#include "../misc/config.h"
#include "../logging/log.h"
#include "../election/common.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

// TODO: maybe remove this?
using NElection::TPeerId;
using NElection::TPeerPriority;
using NElection::TEpoch;

////////////////////////////////////////////////////////////////////////////////

//! A special value indicating that the number of records in the previous
//! changelog is undetermined since there is no previous changelog.
/*!
 *  \see TRecovery
 */
const i32 NonexistingPrevRecordCount = -1;

//! A special value indicating that the number of records in the previous changelog
//! is unknown.
/*!
 *  \see TRecovery
 */
const i32 UnknownPrevRecordCount = -2;

//! A special value indicating that no snapshot id is known.
//! is unknown.
/*!
 *  \see TSnapshotStore
 */
const i32 NonexistingSnapshotId = -1;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EPeerStatus, 
    (Stopped)
    (Elections)
    (FollowerRecovery)
    (Following)
    (LeaderRecovery)
    (Leading)
);

DECLARE_ENUM(ECommitResult,
    (Committed)
    (MaybeCommitted)
    (NotCommitted)
    (InvalidStatus)
    (ReadOnly)
);

DECLARE_ENUM(ECommitMode,
    (NeverFails)
    (MayFail)
);

////////////////////////////////////////////////////////////////////////////////
// TODO: refactor

struct TCellConfig
{
    yvector<Stroka> Addresses;
    TPeerId Id;

    TCellConfig()
        : Id(NElection::InvalidPeerId)
    { }

    void Read(TJsonObject* json)
    {
        NYT::TryRead(json, L"Id", &Id);
        NYT::TryRead(json, L"Addresses", &Addresses);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Describes a configuration of TMetaStateManager.
struct TMetaStateManagerConfig
{
    //! A path where changelogs are stored.
    Stroka LogLocation;

    //! A path where snapshots are stored.
    Stroka SnapshotLocation;

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
    TCellConfig Cell;

    TMetaStateManagerConfig()
        : LogLocation(".")
        , SnapshotLocation(".")
        , MaxChangesBetweenSnapshots(-1)
        , SyncTimeout(TDuration::MilliSeconds(5000))
        , RpcTimeout(TDuration::MilliSeconds(3000))
    { }

    void Read(TJsonObject* json)
    {
        TryRead(json, L"LogLocation", &LogLocation);
        TryRead(json, L"SnapshotLocation", &SnapshotLocation);
        TryRead(json, L"MaxChangesBetweenSnapshots", &MaxChangesBetweenSnapshots);
        auto cellConfig = GetSubTree(json, "Cell");
        if (cellConfig != NULL) {
            Cell.Read(cellConfig);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMetaVersion
{
    i32 SegmentId;
    i32 RecordCount;

    TMetaVersion();
    TMetaVersion(i32 segmentId, i32 recordCount);

    bool operator < (const TMetaVersion& other) const;
    bool operator == (const TMetaVersion& other) const;
    bool operator != (const TMetaVersion& other) const;
    bool operator > (const TMetaVersion& other) const;
    bool operator <= (const TMetaVersion& other) const;
    bool operator >= (const TMetaVersion& other) const;

    Stroka ToString() const
    {
        return Sprintf("%d:%d", SegmentId, RecordCount);
    }
};

inline TMetaVersion::TMetaVersion()
    : SegmentId(0)
    , RecordCount(0)
{  }

inline TMetaVersion::TMetaVersion(i32 segmentId, i32 recordCount)
    : SegmentId(segmentId)
    , RecordCount(recordCount)
{  }

inline bool TMetaVersion::operator < (const TMetaVersion& other) const
{
    return ((SegmentId < other.SegmentId) ||
        (SegmentId == other.SegmentId && RecordCount < other.RecordCount));
}

inline bool TMetaVersion::operator == (const TMetaVersion& other) const
{
    return (SegmentId == other.SegmentId && RecordCount == other.RecordCount);
}

inline bool TMetaVersion::operator != (const TMetaVersion& other) const
{
    return !(*this == other);
}

inline bool TMetaVersion::operator > (const TMetaVersion& other) const
{
    return !(*this <= other);
}

inline bool TMetaVersion::operator <= (const TMetaVersion& other) const
{
    return (*this < other || *this == other);
}

inline bool TMetaVersion::operator >= (const TMetaVersion& other) const
{
    return !(*this < other);
}

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
