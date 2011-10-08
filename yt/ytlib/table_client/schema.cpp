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

TRange::TRange(TColumn begin, TColumn end)
    : Begin_(begin)
    , End_(end)
{
    YASSERT(begin < end);
}

TRange::TRange(TColumn begin)
    : Begin_(begin)
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

void TRange::FillProto(NProto::TRange* protoRange) const
{
    protoRange->SetBegin(Begin_);
    protoRange->SetEnd(Begin_);
}

bool TRange::Contains(TColumn value) const
{
    if (value < Begin_)
        return false;

    if (!IsOpen() && value >= End_)
        return false;

    return true;
}

bool TRange::Overlaps(const TRange& range) const
{
    if ((Begin_ <= range.Begin_ && (IsOpen() || range.Begin_ < End_)) || 
        (Begin_ < range.End_ && (IsOpen() || range.End_ <= End_)) ||
        (range.Begin_ <= Begin_ && (range.IsOpen() || Begin_ < range.End_)))
    {
        return true;
    } else {
        return false;
    }
}

bool TRange::IsOpen() const
{
    return End_.Empty();
}

////////////////////////////////////////////////////////////////////////////////

TChannel& TChannel::AddColumn(TColumn column)
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

TChannel& TChannel::AddRange(TColumn begin, TColumn end)
{
    Ranges_.push_back(TRange(begin, end));
    return *this;
}

void TChannel::FillProto(NProto::TChannel* protoChannel) const
{
    FOREACH(auto column, Columns_) {
        protoChannel->AddColumns(~column);
    }

    FOREACH(const auto& range, Ranges_) {
        auto* protoRange = protoChannel->AddRanges();
        range.FillProto(protoRange);
    }
}

TChannel& TChannel::operator-= (const TChannel& rhsChannel)
{
    yvector<TColumn> newColumns;
    FOREACH(auto column, Columns_) {
        if (!rhsChannel.Contains(column)) {
            newColumns.push_back(column);
        }
    }
    Columns_.swap(newColumns);

    yvector<TRange> rhsRanges(rhsChannel.Ranges_);
    FOREACH(auto column, rhsChannel.Columns_) {
        // add single columns as ranges
        rhsRanges.push_back(TRange(column, NextColumn(column)));
    }

    yvector<TRange> newRanges;
    FOREACH(auto& rhs, rhsRanges) {
        FOREACH(auto& lhs, Ranges_) {
            if (!lhs.Overlaps(rhs)) {
                newRanges.push_back(lhs);
                continue;
            } 

            if (lhs.Begin() < rhs.Begin()) {
                newRanges.push_back(TRange(lhs.Begin(), rhs.Begin()));
            }

            if (rhs.IsOpen()) {
                continue;
            }

            if (lhs.IsOpen()) {
                newRanges.push_back(TRange(rhs.End()));
            } else if (lhs.End() > rhs.End()) {
                newRanges.push_back(TRange(rhs.End(), lhs.End()));
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

bool TChannel::Contains(TColumn column) const
{
    FOREACH(auto& oldColumn, Columns_) {
        if (oldColumn == column) {
            return true;
        }
    }
    return ContainsInRanges(column);
}

bool TChannel::ContainsInRanges(TColumn column) const
{
    FOREACH(auto& range, Ranges_) {
        if (range.Contains(column)) {
            return true;
        }
    }
    return false;
}

const yvector<TColumn>& TChannel::Columns()
{
    return Columns_;
}

////////////////////////////////////////////////////////////////////////////////

TSchema::TSchema()
    // NB: this "trash" channel will be present in any chunk,
    // cause this is how table writer works now. But if it's empty,
    // its blocks will be successfully compressed
    : Channels_(1, TChannel().AddRange("", ""))
{ }

TSchema& TSchema::AddChannel(const TChannel& channel)
{
    Channels_.front() -= channel; 
    Channels_.push_back(channel);
    return *this;
}

const yvector<TChannel>& TSchema::Channels() const
{
    return Channels_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
