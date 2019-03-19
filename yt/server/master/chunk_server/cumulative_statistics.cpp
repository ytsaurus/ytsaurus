#include "cumulative_statistics.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void TCumulativeStatisticsEntry::Persist(const NYT::TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, RowCount);
    Persist(context, ChunkCount);
    Persist(context, DataSize);
}

TCumulativeStatisticsEntry::TCumulativeStatisticsEntry()
{ }

TCumulativeStatisticsEntry::TCumulativeStatisticsEntry(const TChunkTreeStatistics& statistics)
    : RowCount(statistics.LogicalRowCount)
    , ChunkCount(statistics.LogicalChunkCount)
    , DataSize(statistics.UncompressedDataSize)
{ }

TCumulativeStatisticsEntry::TCumulativeStatisticsEntry(i64 rowCount, i64 chunkCount, i64 dataSize)
    : RowCount(rowCount)
    , ChunkCount(chunkCount)
    , DataSize(dataSize)
{ }

TCumulativeStatisticsEntry TCumulativeStatisticsEntry::operator+(const TCumulativeStatisticsEntry& other) const
{
    return TCumulativeStatisticsEntry{
        RowCount + other.RowCount,
        ChunkCount + other.ChunkCount,
        DataSize + other.DataSize
    };
}

TCumulativeStatisticsEntry TCumulativeStatisticsEntry::operator-(const TCumulativeStatisticsEntry& other) const
{
    return TCumulativeStatisticsEntry{
        RowCount - other.RowCount,
        ChunkCount - other.ChunkCount,
        DataSize - other.DataSize
    };
}

bool TCumulativeStatisticsEntry::operator==(const TCumulativeStatisticsEntry& other) const
{
    return RowCount == other.RowCount &&
        ChunkCount == other.ChunkCount &&
        DataSize == other.DataSize;
}

TString ToString(const TCumulativeStatisticsEntry& entry)
{
    return ConvertToYsonString(entry, EYsonFormat::Text).GetData();
}

void Serialize(const TCumulativeStatisticsEntry& entry, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("row_count").Value(entry.RowCount)
            .Item("chunk_count").Value(entry.ChunkCount)
            .Item("data_size").Value(entry.DataSize)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void TCumulativeStatistics::DeclareAppendable()
{
    YCHECK(Empty());
    Statistics_ = TAppendableCumulativeStatistics{};
}

void TCumulativeStatistics::DeclareModifiable()
{
    YCHECK(Empty());
    Statistics_ = TModifyableCumulativeStatistics{};
}

void TCumulativeStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Statistics_);
}

void TCumulativeStatistics::PushBack(const TCumulativeStatisticsEntry& entry)
{
    if (IsAppendable()) {
        auto& statistics = AsAppendable();
        if (statistics.empty()) {
            statistics.push_back(entry);
        } else {
            statistics.push_back(entry + statistics.back());
        }
    } else {
        AsModifiable().PushBack(entry);
    }
}

void TCumulativeStatistics::PopBack()
{
    if (IsAppendable()) {
        AsAppendable().pop_back();
    } else {
        AsModifiable().PopBack();
    }
}

i64 TCumulativeStatistics::Size() const
{
    if (IsAppendable()) {
        return AsAppendable().size();
    } else {
        return AsModifiable().Size();
    }
}

bool TCumulativeStatistics::Empty() const
{
    return Size() == 0;
}

void TCumulativeStatistics::Clear()
{
    if (IsAppendable()) {
        AsAppendable().clear();
    } else {
        AsModifiable().Clear();
    }
}

int TCumulativeStatistics::LowerBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const
{
    auto comparator = [&member] (const TCumulativeStatisticsEntry& lhs, i64 rhs) {
        return lhs.*member < rhs;
    };

    if (IsAppendable()) {
        return std::lower_bound(
            AsAppendable().begin(),
            AsAppendable().end(),
            value,
            comparator) - AsAppendable().begin();
    } else {
        return std::max(0, AsModifiable().LowerBound(value, comparator) - 1);
    }
}

int TCumulativeStatistics::UpperBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const
{
    auto comparator = [&member] (i64 lhs, const TCumulativeStatisticsEntry& rhs) {
        return lhs < rhs.*member;
    };

    if (IsAppendable()) {
        return std::upper_bound(
            AsAppendable().begin(),
            AsAppendable().end(),
            value,
            comparator) - AsAppendable().begin();
    } else {
        return std::max(0, AsModifiable().UpperBound(value, comparator) - 1);
    }
}

TCumulativeStatisticsEntry TCumulativeStatistics::operator[](int index) const
{
    if (IsAppendable()) {
        return AsAppendable()[index];
    } else {
        return AsModifiable().GetCumulativeSum(index + 1);
    }
}

TCumulativeStatisticsEntry TCumulativeStatistics::Back() const
{
    YCHECK(!Empty());
    return this->operator[](Size() - 1);
}

void TCumulativeStatistics::EraseFromFront(int entriesCount)
{
    auto& statistics = AsAppendable();
    statistics.erase(
        statistics.begin(),
        std::min(statistics.begin() + entriesCount, statistics.end()));
}

void TCumulativeStatistics::Update(int index, const TCumulativeStatisticsEntry& delta)
{
    if (IsAppendable()) {
        auto& statistics = AsAppendable();
        YCHECK(index == statistics.size() - 1);
        statistics[index] = statistics[index] + delta;
    } else {
        auto& statistics = AsModifiable();
        YCHECK(index < statistics.Size());
        statistics.Increment(index, delta);
    }
}

bool TCumulativeStatistics::IsAppendable() const
{
    return std::holds_alternative<TAppendableCumulativeStatistics>(Statistics_);
}

bool TCumulativeStatistics::IsModifiable() const
{
    return std::holds_alternative<TModifyableCumulativeStatistics>(Statistics_);
}

TCumulativeStatistics::TAppendableCumulativeStatistics& TCumulativeStatistics::AsAppendable()
{
    YCHECK(IsAppendable());
    return std::get<TAppendableCumulativeStatistics>(Statistics_);
}

const TCumulativeStatistics::TAppendableCumulativeStatistics& TCumulativeStatistics::AsAppendable() const
{
    YCHECK(IsAppendable());
    return std::get<TAppendableCumulativeStatistics>(Statistics_);
}

TCumulativeStatistics::TModifyableCumulativeStatistics& TCumulativeStatistics::AsModifiable()
{
    YCHECK(IsModifiable());
    return std::get<TModifyableCumulativeStatistics>(Statistics_);
}

const TCumulativeStatistics::TModifyableCumulativeStatistics& TCumulativeStatistics::AsModifiable() const
{
    YCHECK(IsModifiable());
    return std::get<TModifyableCumulativeStatistics>(Statistics_);
}

void Serialize(const TCumulativeStatistics& statistics, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    for (int index = 0; index < statistics.Size(); ++index) {
        consumer->OnListItem();
        Serialize(statistics[index], consumer);
    }
    consumer->OnEndList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
