#pragma once

#include "../misc/common.h"
#include "../misc/ptr.h"
#include "value.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRange
{
public:
    TRange(const TValue& begin, const TValue& end, bool closedEnd = false);

    const TValue& Begin() const;
    const TValue& End() const;

    bool Match(const TValue& value) const;
    bool Overlap(const TRange& range) const;

private:
    TValue Begin_;
    TValue End_;
};

////////////////////////////////////////////////////////////////////////////////

// Part of schema descriptions
// Set of fixed columns and column ranges
class TChannel
{
public:
    TChannel& AddColumn(const TValue& column);
    TChannel& AddRange(const TRange& range);
    TChannel& AddRange(const TValue& begin, const TValue& end);

    bool Match(const TValue& column) const;
    bool MatchRanges(const TValue& column) const;

    const yvector<TValue>& Columns();
    const yvector<TRange>& Ranges();

    TChannel& operator-= (const TChannel& channel);
    const TChannel operator- (const TChannel& channel);

private:
    yvector<TValue> Columns_;
    yvector<TRange> Ranges_;
};

////////////////////////////////////////////////////////////////////////////////

class TSchema
{
public:
    typedef TAutoPtr<TSchema> TPtr;

    TSchema();
    TSchema& AddChannel(const TChannel& channel);

    int GetChannelCount() const;
    const yvector<TChannel>& Channels() const;

private:
    yvector<TChannel> Channels_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT