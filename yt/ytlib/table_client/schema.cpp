#include "stdafx.h"
#include "schema.h"

#include <ytlib/misc/error.h>
#include <ytlib/misc/foreach.h>

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/convert.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TRange::TRange(const Stroka& begin, const Stroka& end)
    : IsInfinite_(false)
    , Begin_(begin)
    , End_(end)
{
    if (begin >= end) {
        THROW_ERROR_EXCEPTION("Invalid range: [%s,%s]",
            ~begin,
            ~end);
    }
}

TRange::TRange(const Stroka& begin)
    : IsInfinite_(true)
    , Begin_(begin)
    , End_("")
{ }

Stroka TRange::Begin() const
{
    return Begin_;
}

Stroka TRange::End() const
{
    return End_;
}

NProto::TRange TRange::ToProto() const
{
    NProto::TRange protoRange;
    protoRange.set_begin(Begin_);
    protoRange.set_end(End_);
    protoRange.set_is_infinite(IsInfinite_);
    return protoRange;
}

TRange TRange::FromProto(const NProto::TRange& protoRange)
{
    if (protoRange.is_infinite()) {
        return TRange(protoRange.begin());
    } else {
        return TRange(protoRange.begin(), protoRange.end());
    }
}

bool TRange::Contains(const TStringBuf& value) const
{
    if (value < Begin_)
        return false;

    if (!IsInfinite() && value >= End_)
        return false;

    return true;
}

bool TRange::Contains(const TRange& range) const
{
    if (range.IsInfinite()) {
        return Contains(range.Begin()) && IsInfinite();
    } else if (IsInfinite()) {
        return Contains(range.Begin());
    } else {
        return Contains(range.Begin()) && range.End() <= End_;
    }
}

bool TRange::Overlaps(const TRange& range) const
{
    return 
        ( Begin_ <= range.Begin_ && (IsInfinite() || range.Begin_ <  End_) ) || 
        ( Begin_ <  range.End_   && (IsInfinite() || range.End_   <= End_) ) ||
        ( Begin_ >= range.Begin_ && (range.IsInfinite() || range.End_ > Begin_) );
}

bool TRange::IsInfinite() const
{
    return IsInfinite_;
}

////////////////////////////////////////////////////////////////////////////////

TChannel::TChannel()
{ }

void TChannel::AddColumn(const Stroka& column)
{
    FOREACH (const auto& existingColumn, Columns_) {
        if (existingColumn == column) {
            return;
        }
    }

    Columns_.push_back(column);
}

void TChannel::AddRange(const TRange& range)
{
    Ranges_.push_back(range);
}

void TChannel::AddRange(const Stroka& begin, const Stroka& end)
{
    Ranges_.push_back(TRange(begin, end));
}

NProto::TChannel TChannel::ToProto() const
{
    NProto::TChannel protoChannel;
    FOREACH (const auto& column, Columns_) {
        protoChannel.add_columns(~column);
    }

    FOREACH (const auto& range, Ranges_) {
        *protoChannel.add_ranges() = range.ToProto();
    }
    return protoChannel;
}

NYT::NTableClient::TChannel TChannel::FromProto(const NProto::TChannel& protoChannel)
{
    TChannel result;
    for (int i = 0; i < protoChannel.columns_size(); ++i) {
        result.AddColumn(protoChannel.columns(i));
    }

    for (int i = 0; i < protoChannel.ranges_size(); ++i) {
        result.AddRange(TRange::FromProto(protoChannel.ranges(i)));
    }
    return result;
}

bool TChannel::Contains(const TStringBuf& column) const
{
    FOREACH (const auto& oldColumn, Columns_) {
        if (oldColumn == column) {
            return true;
        }
    }
    return ContainsInRanges(column);
}

bool TChannel::Contains(const TRange& range) const
{
    FOREACH (const auto& currentRange, Ranges_) {
        if (currentRange.Contains(range)) {
            return true;
        }
    }
    return false;
}

bool TChannel::Contains(const TChannel& channel) const
{
    FOREACH (const auto& column, channel.Columns_) {
        if (!Contains(column)) {
            return false;
        }
    }

    FOREACH (const auto& range, channel.Ranges_) {
        if (!Contains(range)) {
            return false;
        }
    }

    return true;
}

bool TChannel::ContainsInRanges(const TStringBuf& column) const
{
    FOREACH (const auto& range, Ranges_) {
        if (range.Contains(column)) {
            return true;
        }
    }
    return false;
}

bool TChannel::Overlaps(const TRange& range) const
{
    FOREACH (const auto& column, Columns_) {
        if (range.Contains(column)) {
            return true;
        }
    }

    FOREACH (const auto& currentRange, Ranges_) {
        if (currentRange.Overlaps(range)) {
            return true;
        }
    }

    return false;
}

bool TChannel::Overlaps(const TChannel& channel) const
{
    FOREACH (const auto& column, channel.Columns_) {
        if (Contains(column)) {
            return true;
        }
    }

    FOREACH (const auto& range, channel.Ranges_) {
        if (Overlaps(range)) {
            return true;
        }
    }

    return false;
}

const std::vector<Stroka>& TChannel::GetColumns() const
{
    return Columns_;
}

bool TChannel::IsEmpty() const
{
    return Columns_.empty() && Ranges_.empty();
}

namespace {

TChannel CreateUniversal()
{
    TChannel result;
    result.AddRange(TRange(""));
    return result;   
}

TChannel CreateEmpty()
{
    return TChannel();
}

} // namespace

const TChannel& TChannel::Universal()
{
    static auto result = CreateUniversal();
    return result;
}

const TChannel& TChannel::Empty()
{
    static auto result = CreateEmpty();
    return result;
}

TChannel& operator -= (TChannel& lhs, const TChannel& rhs)
{
    std::vector<Stroka> newColumns;
    FOREACH (const auto& column, lhs.Columns_) {
        if (!rhs.Contains(column)) {
            newColumns.push_back(column);
        }
    }
    lhs.Columns_.swap(newColumns);

    std::vector<TRange> rhsRanges(rhs.Ranges_);
    FOREACH (const auto& column, rhs.Columns_) {
        // Add single columns as ranges.
        Stroka rangeEnd;
        rangeEnd.reserve(column.Size() + 1);
        rangeEnd.append(column);
        rangeEnd.append('\0');
        rhsRanges.push_back(TRange(column, rangeEnd));
    }

    std::vector<TRange> newRanges;
    FOREACH (const auto& rhsRange, rhsRanges) {
        FOREACH (const auto& lhsRange, lhs.Ranges_) {
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
        lhs.Ranges_.swap(newRanges);
        newRanges.clear();
    }

    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TChannel& channel, INodePtr node)
{
    if (node->GetType() != ENodeType::List) {
        THROW_ERROR_EXCEPTION("Channel description can only be parsed from a list");
    }

    channel = TChannel::Empty();
    FOREACH (auto child, node->AsList()->GetChildren()) {
        switch (child->GetType()) {
            case ENodeType::String:
                channel.AddColumn(child->GetValue<Stroka>());
                break;

            case ENodeType::List: {
                auto listChild = child->AsList();
                switch (listChild->GetChildCount()) {
                    case 1: {
                        auto item = listChild->GetChild(0);
                        if (item->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Channel range description cannot contain %s items",
                                ~item->GetType().ToString().Quote());
                        }
                        channel.AddRange(TRange(item->GetValue<Stroka>()));
                        break;
                    }

                    case 2: {
                        auto itemLo = listChild->GetChild(0);
                        if (itemLo->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Channel range description cannot contain %s items",
                                ~itemLo->GetType().ToString().Quote());
                        }
                        auto itemHi = listChild->GetChild(1);
                        if (itemHi->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Channel range description cannot contain %s items",
                                ~itemHi->GetType().ToString().Quote());
                        }
                        channel.AddRange(TRange(itemLo->GetValue<Stroka>(), itemHi->GetValue<Stroka>()));
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Channel range description cannot contain %d items",
                            listChild->GetChildCount());
                };
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Channel description cannot contain %s items",
                    ~child->GetType().ToString().Quote());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
