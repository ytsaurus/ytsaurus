#pragma once

#include "../misc/common.h"
#include "../misc/configurable.h"
#include "../logging/log.h"
#include "../election/common.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger MetaStateLogger;

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

////////////////////////////////////////////////////////////////////////////////

struct TCellConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TCellConfig> TPtr;

    //! Master server addresses.
    yvector<Stroka> Addresses;

    //! The current master server id.
    TPeerId Id;

    TCellConfig()
    {
        Register("id", Id).Default(NElection::InvalidPeerId);
        Register("addresses", Addresses).NonEmpty();
    }

    virtual void Validate(const NYTree::TYPath& path = "") const
    {
        TConfigurable::Validate(path);
        if (Id == NElection::InvalidPeerId) {
            ythrow yexception() << "Missing peer id";
        }
        if (Id < 0 || Id >= Addresses.ysize()) {
            ythrow yexception() << Sprintf("Id must be in range 0..%d", Addresses.ysize() - 1);
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
    return
        SegmentId < other.SegmentId ||
        SegmentId == other.SegmentId && RecordCount < other.RecordCount;
}

inline bool TMetaVersion::operator == (const TMetaVersion& other) const
{
    return SegmentId == other.SegmentId && RecordCount == other.RecordCount;
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
    return *this < other || *this == other;
}

inline bool TMetaVersion::operator >= (const TMetaVersion& other) const
{
    return !(*this < other);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
