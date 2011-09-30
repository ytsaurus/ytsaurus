#pragma once

#include "value.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TRangeOf
{
    T BeginK;
    T EndK;

public:

    TRangeOf(const T& begin, const T& end, bool closedEnd = false)
        : BeginK(begin)
        , EndK(end)
    {
        if (closedEnd)
            EndK = MakeOpenFromClosed(end);
    }

    const T& Begin() const { return BeginK; }
    const T& End() const { return EndK; }

    bool Match(const T& val) const
    {
        if (BeginK.IsSet() && val < BeginK)
            return false;

        if (EndK.IsSet() && val >= EndK)
            return false;

        return true;
    }

    // ToDo: make better name
    bool Intersects(const TRangeOf& range) const
    {
        if ((BeginK <= range.BeginK && (!EndK.IsSet() || range.BeginK < EndK)) || 
            (BeginK < range.EndK && (!EndK.IsSet() || range.EndK <= EndK)) ||
            (range.BeginK <= BeginK && (!range.EndK.IsSet() || BeginK < range.EndK)))
            return true;
        else
            return false;
    }
};

////////////////////////////////////////////////////////////////////////////////
// range of column names (holder type only)

template<class T>
TRangeOf<T> UniteRanges(const TRangeOf<T>& lho, const TRangeOf<T>& rho)
{
    /*YASSERT(Intersects(range));

    if (range.BeginK < BeginK)
        BeginK = range.BeginK;
    if (EndK.IsSet() && (!range.EndK.IsSet() || (range.EndK > EndK)))
        EndK = range.EndK;*/
}

typedef TRangeOf<TValue> TRange;

TValue MakeOpenFromClosed(const TValue& value);

TRange Range(const TValue& begin, const TValue& end, bool closedEnd = false);
TRange RayRange(const TValue& begin);
TRange PrefixRange(const TValue& prefix);

////////////////////////////////////////////////////////////////////////////////

// channel: type used for schema descriptions
// set of stand-alone columns and column ranges

class TChannel
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel();
    void AddColumn(const TValue& column);
    void AddRange(const TRange& range);

    bool IsEmpty() const;

    bool Match(const TValue& column) const;
    bool Match(const TRange& range) const;
    bool Match(const TChannel& channel) const;

    bool Intersect(const TRange& range) const;
    bool Intersect(const TChannel& channel) const;

    TChannel& operator-= (const TChannel& channel);
    const TChannel operator- (const TChannel& channel);

private:
    yvector<TValue> Columns;
    yvector<TRange> Ranges;
};

////////////////////////////////////////////////////////////////////////////////

class TSchema
{
public:
    TSchema();
    void AddChannel(TChannel::TPtr channel);

    const TChannel& GetChannel(int channelIndex) const;

    int GetChannelCount() const;

private:
    yvector<TChannel::TPtr> Channels;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT