#include "stdafx.h"
#include "schema.h"

#include <core/misc/error.h>

#include <core/ytree/node.h>
#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>

namespace NYT {
namespace NChunkClient {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TRange::TRange(const Stroka& begin, const Stroka& end)
    : IsInfinite_(false)
    , Begin_(begin)
    , End_(end)
{
    if (begin >= end) {
        THROW_ERROR_EXCEPTION("Invalid range [%Qv,%Qv]",
            begin,
            end);
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

TChannel::TChannel(
    const std::vector<Stroka> columns,
    std::vector<TRange> ranges)
    : Columns_(std::move(columns))
    , Ranges_(std::move(ranges))
{ }

void TChannel::AddColumn(const Stroka& column)
{
    for (const auto& existingColumn : Columns_) {
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

bool TChannel::Contains(const TStringBuf& column) const
{
    for (const auto& oldColumn : Columns_) {
        if (oldColumn == column) {
            return true;
        }
    }
    return ContainsInRanges(column);
}

bool TChannel::Contains(const TRange& range) const
{
    for (const auto& currentRange : Ranges_) {
        if (currentRange.Contains(range)) {
            return true;
        }
    }
    return false;
}

bool TChannel::Contains(const TChannel& channel) const
{
    for (const auto& column : channel.Columns_) {
        if (!Contains(column)) {
            return false;
        }
    }

    for (const auto& range : channel.Ranges_) {
        if (!Contains(range)) {
            return false;
        }
    }

    return true;
}

bool TChannel::ContainsInRanges(const TStringBuf& column) const
{
    for (const auto& range : Ranges_) {
        if (range.Contains(column)) {
            return true;
        }
    }
    return false;
}

bool TChannel::Overlaps(const TRange& range) const
{
    for (const auto& column : Columns_) {
        if (range.Contains(column)) {
            return true;
        }
    }

    for (const auto& currentRange : Ranges_) {
        if (currentRange.Overlaps(range)) {
            return true;
        }
    }

    return false;
}

bool TChannel::Overlaps(const TChannel& channel) const
{
    for (const auto& column : channel.Columns_) {
        if (Contains(column)) {
            return true;
        }
    }

    for (const auto& range : channel.Ranges_) {
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

const std::vector<TRange>& TChannel::GetRanges() const
{
    return Ranges_;
}

bool TChannel::IsEmpty() const
{
    return Columns_.empty() && Ranges_.empty();
}

bool TChannel::IsUniversal() const
{
    return Columns_.empty() &&
           Ranges_.size() == 1 &&
           Ranges_[0].Begin() == "" &&
           Ranges_[0].IsInfinite();
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
    for (const auto& column : lhs.Columns_) {
        if (!rhs.Contains(column)) {
            newColumns.push_back(column);
        }
    }
    lhs.Columns_.swap(newColumns);

    std::vector<TRange> rhsRanges(rhs.Ranges_);
    for (const auto& column : rhs.Columns_) {
        // Add single columns as ranges.
        Stroka rangeEnd;
        rangeEnd.reserve(column.Size() + 1);
        rangeEnd.append(column);
        rangeEnd.append('\0');
        rhsRanges.push_back(TRange(column, rangeEnd));
    }

    std::vector<TRange> newRanges;
    for (const auto& rhsRange : rhsRanges) {
        for (const auto& lhsRange : lhs.Ranges_) {
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

void ToProto(NProto::TRange* protoRange, const TRange& range)
{
    protoRange->set_begin(range.Begin());
    protoRange->set_end(range.End());
    protoRange->set_infinite(range.IsInfinite());
}

void FromProto(TRange* range, const NProto::TRange& protoRange)
{
    *range = protoRange.infinite()
        ? TRange(protoRange.begin())
        : TRange(protoRange.begin(), protoRange.end());
}

void ToProto(NProto::TChannel* protoChannel, const TChannel& channel)
{
    NYT::ToProto(protoChannel->mutable_columns(), channel.GetColumns());
    NYT::ToProto(protoChannel->mutable_ranges(), channel.GetRanges());
}

void FromProto(TChannel* channel, const NProto::TChannel& protoChannel)
{
    *channel = TChannel(
        NYT::FromProto<Stroka>(protoChannel.columns()),
        NYT::FromProto<TRange>(protoChannel.ranges()));
}

void Deserialize(TChannel& channel, INodePtr node)
{
    if (node->GetType() != ENodeType::List) {
        THROW_ERROR_EXCEPTION("Channel description can only be parsed from a list");
    }

    channel = TChannel::Empty();
    for (auto child : node->AsList()->GetChildren()) {
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
                            THROW_ERROR_EXCEPTION("Channel range description cannot contain %Qv items",
                                item->GetType());
                        }
                        channel.AddRange(TRange(item->GetValue<Stroka>()));
                        break;
                    }

                    case 2: {
                        auto itemLo = listChild->GetChild(0);
                        if (itemLo->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Channel range description cannot contain %Qv items",
                                itemLo->GetType());
                        }
                        auto itemHi = listChild->GetChild(1);
                        if (itemHi->GetType() != ENodeType::String) {
                            THROW_ERROR_EXCEPTION("Channel range description cannot contain %Qv items",
                                itemHi->GetType());
                        }
                        channel.AddRange(TRange(itemLo->GetValue<Stroka>(), itemHi->GetValue<Stroka>()));
                        break;
                    }

                    default:
                        THROW_ERROR_EXCEPTION("Channel range description cannot contain %v items",
                            listChild->GetChildCount());
                };
                break;
            }

            default:
                THROW_ERROR_EXCEPTION("Channel description cannot contain %Qv items",
                    child->GetType());
        }
    }
}

void Serialize(const TChannel& channel, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginList()
        .DoFor(channel.GetColumns(), [] (TFluentList fluent, Stroka column) {
            fluent.Item().Value(column);
        })
        .DoFor(channel.GetRanges(), [] (TFluentList fluent, const TRange& range) {
            fluent.Item()
                .BeginList()
                    .Item().Value(range.Begin())
                    .DoIf(!range.IsInfinite(), [&] (TFluentList fluent) {fluent
                        .Item().Value(range.End());
                    })
                .EndList();
        })
        .EndList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
