#include "schema.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

TColumn NextColumn(TColumn column)
{
    if (column.Empty()) {
        return "\0";
    }

    // ToDo: need code review
    TColumn result;
    if (column.back() != 0xFF) {
        result.reserve(column.Size());
        result.append(~column, column.Size() - 1);
        result.append(column.back() + 1);
    } else {
        result.reserve(column.Size() + 1);
        result.append(column);
        result.append('\0');
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TRange::TRange(const TColumn& begin, const TColumn& end)
    : IsInfinite_(false)
    , Begin_(begin)
    , End_(end)
{
    YASSERT(begin < end);
}

TRange::TRange(const TColumn& begin)
    : IsInfinite_(false)
    , Begin_(begin)
    , End_("")
{ }

TColumn TRange::Begin() const
{
    return Begin_;
}

TColumn TRange::End() const
{
    return End_;
}

NProto::TRange TRange::ToProto() const
{
    NProto::TRange protoRange;
    protoRange.SetBegin(Begin_);
    protoRange.SetEnd(End_);
    return protoRange;
}

bool TRange::Contains(const TColumn& value) const
{
    if (value < Begin_)
        return false;

    if (!IsInfinite() && value >= End_)
        return false;

    return true;
}

bool TRange::Overlaps(const TRange& range) const
{
    if ((Begin_ <= range.Begin_ && (IsInfinite() || range.Begin_ < End_)) || 
        (Begin_ < range.End_ && (IsInfinite() || range.End_ <= End_)) ||
        (range.Begin_ <= Begin_ && (range.IsInfinite() || Begin_ < range.End_)))
    {
        return true;
    } else {
        return false;
    }
}

bool TRange::IsInfinite() const
{
    return IsInfinite_;
}

////////////////////////////////////////////////////////////////////////////////

void TChannel::AddColumn(const TColumn& column)
{
    FOREACH(auto& oldColumn, Columns){
        if (oldColumn == column) {
            return;
        }
    }

    Columns.push_back(column);
}

void TChannel::AddRange(const TRange& range)
{
    Ranges.push_back(range);
}

void TChannel::AddRange(const TColumn& begin, const TColumn& end)
{
    Ranges.push_back(TRange(begin, end));
}

NProto::TChannel TChannel::ToProto() const
{
    NProto::TChannel protoChannel;
    FOREACH(auto column, Columns) {
        protoChannel.AddColumns(~column);
    }

    FOREACH(const auto& range, Ranges) {
        *protoChannel.AddRanges() = range.ToProto();
    }
    return protoChannel;
}

bool TChannel::Contains(const TColumn& column) const
{
    FOREACH(auto& oldColumn, Columns) {
        if (oldColumn == column) {
            return true;
        }
    }
    return ContainsInRanges(column);
}

bool TChannel::ContainsInRanges(const TColumn& column) const
{
    FOREACH(auto& range, Ranges) {
        if (range.Contains(column)) {
            return true;
        }
    }
    return false;
}

const yvector<TColumn>& TChannel::GetColumns()
{
    return Columns;
}

////////////////////////////////////////////////////////////////////////////////

void operator-= (TChannel& lhs, const TChannel& rhs)
{
    yvector<TColumn> newColumns;
    FOREACH(auto column, lhs.Columns) {
        if (!rhs.Contains(column)) {
            newColumns.push_back(column);
        }
    }
    lhs.Columns.swap(newColumns);

    yvector<TRange> rhsRanges(rhs.Ranges);
    FOREACH(auto column, rhs.Columns) {
        // add single columns as ranges
        rhsRanges.push_back(TRange(column, NextColumn(column)));
    }

    yvector<TRange> newRanges;
    FOREACH(auto& rhsRange, rhsRanges) {
        FOREACH(auto& lhsRange, lhs.Ranges) {
            if (!lhsRange.Overlaps(rhsRange)) {
                newRanges.push_back(lhsRange);
                continue;
            } 

            if (lhsRange.Begin() < rhsRange.Begin()) {
                newRanges.push_back(TRange(lhsRange.Begin(), rhsRange.Begin()));
            }

            if (rhsRange.IsInfinite()) {
                continue;
            }

            if (lhsRange.IsInfinite()) {
                newRanges.push_back(TRange(rhsRange.End()));
            } else if (lhsRange.End() > rhsRange.End()) {
                newRanges.push_back(TRange(rhsRange.End(), lhsRange.End()));
            }
        }
        lhs.Ranges.swap(newRanges);
        newRanges.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSchema::TSchema()
{
    TChannel trashChannel;
    trashChannel.AddRange(TRange(""));

    // NB: this "trash" channel will be present in any chunk,
    // cause this is how table writer works now. But if it's empty,
    // its blocks will be successfully compressed
    Channels.push_back(trashChannel);
}

void TSchema::AddChannel(const TChannel& channel)
{
    Channels.front() -= channel; 
    Channels.push_back(channel);
}

const yvector<TChannel>& TSchema::GetChannels() const
{
    return Channels;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
