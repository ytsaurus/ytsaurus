#include "stdafx.h"
#include "schema.h"
#include "key.h"

#include <core/misc/error.h>

#include <core/ytree/node.h>
#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>

namespace NYT {
namespace NChunkClient {

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

NYT::NChunkClient::TChannel TChannel::FromProto(const NProto::TChannel& protoChannel)
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

void Serialize(const TChannel& channel, NYson::IYsonConsumer* consumer)
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

namespace NProto {

void Serialize(const NProto::TReadLimit& readLimit, NYson::IYsonConsumer* consumer)
{
    int fieldCount = 0;
    if (readLimit.has_row_index())   { fieldCount += 1; }
    if (readLimit.has_key())         { fieldCount += 1; }
    if (readLimit.has_chunk_index()) { fieldCount += 1; }
    if (readLimit.has_offset())      { fieldCount += 1; }

    if (fieldCount == 0) {
        THROW_ERROR_EXCEPTION("Cannot serialize empty read limit");
    }
    if (fieldCount >= 2) {
        THROW_ERROR_EXCEPTION("Cannot serialize read limit with more than one field");
    }

    consumer->OnBeginMap();
    if (readLimit.has_row_index()) {
        consumer->OnKeyedItem("row_index");
        consumer->OnIntegerScalar(readLimit.row_index());
    } else if (readLimit.has_chunk_index()) {
        consumer->OnKeyedItem("chunk_index");
        consumer->OnIntegerScalar(readLimit.chunk_index());
    } else if (readLimit.has_offset()) {
        consumer->OnKeyedItem("offset");
        consumer->OnIntegerScalar(readLimit.offset());
    } else if (readLimit.has_key()) {
        consumer->OnKeyedItem("key");
        consumer->OnBeginList();
        FOREACH (const auto& part, readLimit.key().parts()) {
            consumer->OnListItem();
            switch (part.type()) {
                case EKeyPartType::String:
                    consumer->OnStringScalar(part.str_value());
                    break;
                case EKeyPartType::Integer:
                    consumer->OnIntegerScalar(part.int_value());
                    break;
                case EKeyPartType::Double:
                    consumer->OnDoubleScalar(part.double_value());
                    break;
            }
        }
        consumer->OnEndList();
    }
    consumer->OnEndMap();
}

void Deserialize(NProto::TReadLimit& readLimit, INodePtr node)
{
    if (node->GetType() != ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Unexpected read limit token: %s", ~node->GetType().ToString());
    }

    auto mapNode = node->AsMap();
    if (mapNode->GetChildCount() > 1) {
        THROW_ERROR_EXCEPTION("Too many children in read limit: %d > 1", mapNode->GetChildCount());
    }

    if (auto child = mapNode->FindChild("row_index")) {
        if (child->GetType() != ENodeType::Integer) {
            THROW_ERROR_EXCEPTION("Unexpected row index token: %s", ~child->GetType().ToString());
        }
        readLimit.set_row_index(child->GetValue<i64>());
    } else if (auto child = mapNode->FindChild("chunk_index")) {
        if (child->GetType() != ENodeType::Integer) {
            THROW_ERROR_EXCEPTION("Unexpected chunk index token: %s", ~child->GetType().ToString());
        }
        readLimit.set_chunk_index(child->GetValue<i64>());
    } else if (auto child = mapNode->FindChild("offset")) {
        if (child->GetType() != ENodeType::Integer) {
            THROW_ERROR_EXCEPTION("Unexpected chunk index token: %s", ~child->GetType().ToString());
        }
        readLimit.set_offset(child->GetValue<i64>());
    } else if (auto child = mapNode->FindChild("key")) {
        if (child->GetType() != ENodeType::List) {
            THROW_ERROR_EXCEPTION("Unexpected key token: %s", ~child->GetType().ToString());
        }
        auto key = readLimit.mutable_key();
        auto nodeList = child->AsList();
        for (int i = 0; i < nodeList->GetChildCount(); ++i) {
            auto child = nodeList->FindChild(i);
            auto keyPart = key->add_parts();
            switch (child->GetType()) {
                case ENodeType::String:
                    keyPart->set_type(EKeyPartType::String);
                    keyPart->set_str_value(child->GetValue<Stroka>());
                    break;
                case ENodeType::Integer:
                    keyPart->set_type(EKeyPartType::Integer);
                    keyPart->set_int_value(child->GetValue<i64>());
                    break;
                case ENodeType::Double:
                    keyPart->set_type(EKeyPartType::Double);
                    keyPart->set_double_value(child->GetValue<double>());
                    break;
                default:
                    THROW_ERROR_EXCEPTION("Unexpected key part type: %s", ~child->GetType().ToString());
            }
        }
    }
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
