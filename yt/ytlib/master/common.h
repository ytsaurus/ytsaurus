#pragma once

#include "../misc/common.h"

#include "../logging/log.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: move?
struct TMasterStateId
{
    i32 SegmentId;
    i32 ChangeCount;

    TMasterStateId();
    TMasterStateId(i32 segmentId, i32 ChangeCount);

    bool operator < (const TMasterStateId& other) const;
    bool operator == (const TMasterStateId& other) const;
    bool operator != (const TMasterStateId& other) const;
    bool operator > (const TMasterStateId& other) const;
    bool operator <= (const TMasterStateId& other) const;
    bool operator >= (const TMasterStateId& other) const;

    Stroka ToString() const
    {
        return Sprintf("%d:%d", SegmentId, ChangeCount);
    }
};

inline TMasterStateId::TMasterStateId()
    : SegmentId(0)
    , ChangeCount(0)
{  }

inline TMasterStateId::TMasterStateId(i32 segmentId, i32 recordCount)
    : SegmentId(segmentId)
    , ChangeCount(recordCount)
{  }

inline bool TMasterStateId::operator < (const TMasterStateId& other) const
{
    return ((SegmentId < other.SegmentId) ||
        (SegmentId == other.SegmentId && ChangeCount < other.ChangeCount));
}

inline bool TMasterStateId::operator == (const TMasterStateId& other) const
{
    return (SegmentId == other.SegmentId && ChangeCount == other.ChangeCount);
}

inline bool TMasterStateId::operator != (const TMasterStateId& other) const
{
    return !(*this == other);
}

inline bool TMasterStateId::operator > (const TMasterStateId& other) const
{
    return !(*this <= other);
}

inline bool TMasterStateId::operator <= (const TMasterStateId& other) const
{
    return (*this < other || *this == other);
}

inline bool TMasterStateId::operator >= (const TMasterStateId& other) const
{
    return !(*this < other);
}

////////////////////////////////////////////////////////////////////////////////

typedef i32 TMasterId;
const TMasterId InvalidMasterId = -1;

extern NLog::TLogger MasterLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace
