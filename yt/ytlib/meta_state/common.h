#pragma once

#include "../misc/common.h"
#include "../misc/config.h"
#include "../misc/foreach.h"

#include "../logging/log.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef i32 TPeerId;
const TPeerId InvalidPeerId = -1;

const i32 NonexistingPrevRecordCount = -1;
const i32 UnknownPrevRecordCount = -2;

const i32 NonexistingSnapshotId = -1;

////////////////////////////////////////////////////////////////////////////////
// TODO: refactor

struct TCellConfig
{
    yvector<Stroka> Addresses;
    TPeerId Id;

    TCellConfig()
        : Id(InvalidPeerId)
    { }

    void Read(TJsonObject* json)
    {
        NYT::TryRead(json, L"Id", &Id);
        NYT::TryRead(json, L"Addresses", &Addresses);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TMetaStateManagerConfig
{
    Stroka LogLocation;
    Stroka SnapshotLocation;
    i32 MaxRecordCount;
    TDuration SyncTimeout;
    TDuration RpcTimeout;
    TCellConfig Cell;

    TMetaStateManagerConfig()
        : LogLocation(".")
        , SnapshotLocation(".")
        , MaxRecordCount(100000)
        , SyncTimeout(TDuration::MilliSeconds(5000))
        , RpcTimeout(TDuration::MilliSeconds(3000))
    { }

    void Read(TJsonObject* json)
    {
        TryRead(json, L"LogLocation", &LogLocation);
        TryRead(json, L"SnapshotLocation", &SnapshotLocation);
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

} // namespace
