#include "../misc/foreach.h"
#include "schema.h"

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

TRange::TRange(const TValue& begin, const TValue& end, bool closedEnd)
{
    if (end.IsNull() || begin < end) {
        Begin_ = begin;
        End_ = end;
    } else {
        Begin_ = end;
        End_ = begin;
    }

    if (closedEnd)
        End_ = NextValue(End_);
}

const TValue& TRange::Begin() const
{
    return Begin_;
}

const TValue& TRange::End() const
{
    return End_;
}

bool TRange::Match(const TValue& value) const
{
    if (value < Begin_)
        return false;

    if (!End_.IsNull() && value >= End_)
        return false;

    return true;
}

bool TRange::Overlap(const TRange& range) const
{
    if ((Begin_ <= range.Begin_ && 
        (!End_.IsNull() || range.Begin_ < End_)) || 
        (Begin_ < range.End_ && 
        (!End_.IsNull() || range.End_ <= End_)) ||
        (range.Begin_ <= Begin_ && 
        (!range.End_.IsNull() || Begin_ < range.End_))) 
    {
        return true;
    } else {
        return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TChannel& TChannel::AddColumn(const TValue& column)
{
    FOREACH(auto& oldColumn, Columns_){
        if (oldColumn == column) {
            return *this;
        }
    }

    Columns_.push_back(column);
    return *this;
}

TChannel& TChannel::AddRange(const TRange& range)
{
    Ranges_.push_back(range);
    return *this;
}

TChannel& TChannel::AddRange(const TValue& begin, const TValue& end)
{
    Ranges_.push_back(TRange(begin, end));
    return *this;
}

TChannel& TChannel::operator-= (const TChannel& rhsChannel)
{
    yvector<TValue> newColumns;
    FOREACH(auto& column, Columns_) {
        if (!rhsChannel.Match(column)) {
            newColumns.push_back(column);
        }
    }
    Columns_.swap(newColumns);

    yvector<TRange> rhsRanges(rhsChannel.Ranges_);
    FOREACH(auto& column, rhsChannel.Columns_) {
        // add single columns as ranges
        rhsRanges.push_back(TRange(column, column, true));
    }

    yvector<TRange> newRanges;
    FOREACH(auto& rhs, rhsRanges) {
        FOREACH(auto& lhs, Ranges_) {
            if (!lhs.Overlap(rhs)) {
                newRanges.push_back(lhs);
            } else {
                if (lhs.Begin() < rhs.Begin()) {
                    newRanges.push_back(TRange(lhs.Begin(), rhs.Begin()));
                }

                if (!rhs.End().IsNull() && 
                    (lhs.End().IsNull() || lhs.End() > rhs.End())) 
                {
                    newRanges.push_back(TRange(rhs.End(), lhs.End()));
                }
            }
        }
        Ranges_.swap(newRanges);
        newRanges.clear();
    }

    return *this;
}

const TChannel TChannel::operator- (const TChannel& channel)
{
    return (TChannel(*this) -= channel);
}

bool TChannel::Match(const TValue& column) const
{
    FOREACH(auto& oldColumn, Columns_) {
        if (oldColumn == column) {
            return true;
        }
    }
    return MatchRanges(column);
}

bool TChannel::MatchRanges(const TValue& column) const
{
    FOREACH(auto& range, Ranges_) {
        if (range.Match(column)) {
            return true;
        }
    }
    return false;
}

const yvector<TValue>& TChannel::Columns()
{
    return Columns_;
}

const yvector<TRange>& TChannel::Ranges()
{
    return Ranges_;
}

////////////////////////////////////////////////////////////////////////////////

TSchema::TSchema()
    // NB: this "other" channel will be present in any chunk,
    // cause this is how table writer works now. But if it's empty,
    // its blocks will be successfully compressed
    : Channels_(1, TChannel().AddRange(TValue::Null(), TValue::Null()))
{ }

TSchema& TSchema::AddChannel(const TChannel& channel)
{
    Channels_.front() -= channel; 
    Channels_.push_back(channel);
    return *this;
}

int TSchema::GetChannelCount() const
{
    return Channels_.ysize();
}

const yvector<TChannel>& TSchema::Channels() const
{
    return Channels_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
