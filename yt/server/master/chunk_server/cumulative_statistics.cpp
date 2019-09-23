#include "cumulative_statistics.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

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

bool TCumulativeStatisticsEntry::operator!=(const TCumulativeStatisticsEntry& other) const
{
    return !(*this == other);
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
    YT_VERIFY(Empty());
    Statistics_.emplace<AppendableAlternativeIndex>();
}

void TCumulativeStatistics::DeclareModifiable()
{
    YT_VERIFY(Empty());
    Statistics_.emplace<ModifiableAlternativeIndex>();
}

void TCumulativeStatistics::DeclareTrimmable()
{
    YT_VERIFY(Empty());
    Statistics_.emplace<TrimmableAlternativeIndex>(1);
}

void TCumulativeStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Statistics_);
}

void TCumulativeStatistics::PushBack(const TCumulativeStatisticsEntry& entry)
{
    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex: {
            auto& statistics = AsAppendable();
            if (statistics.empty()) {
                statistics.push_back(entry);
            } else {
                statistics.push_back(entry + statistics.back());
            }
            break;
        }

        case ModifiableAlternativeIndex:
            AsModifiable().PushBack(entry);
            break;

        case TrimmableAlternativeIndex: {
            auto& statistics = AsTrimmable();
            statistics.push_back(entry + statistics.back());
            break;
        }

        default:
            YT_ABORT();
    }
}

void TCumulativeStatistics::PopBack()
{
    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex:
            AsAppendable().pop_back();
            break;

        case ModifiableAlternativeIndex:
            AsModifiable().PopBack();
            break;

        case TrimmableAlternativeIndex:
            AsTrimmable().pop_back();
            break;

        default:
            YT_ABORT();
    }
}

void TCumulativeStatistics::Update(int index, const TCumulativeStatisticsEntry& delta)
{
    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex: {
            auto& statistics = AsAppendable();
            YT_VERIFY(index == statistics.size() - 1);
            statistics[index] = statistics[index] + delta;
            break;
        }

        case ModifiableAlternativeIndex: {
            auto& statistics = AsModifiable();
            YT_VERIFY(index < statistics.Size());
            statistics.Increment(index, delta);
            break;
        }

        case TrimmableAlternativeIndex: {
            auto& statistics = AsTrimmable();
            YT_VERIFY(index == statistics.size() - 2);
            statistics[index + 1] = statistics[index + 1] + delta;
            break;
        }

        default:
            YT_ABORT();
    }
}

i64 TCumulativeStatistics::Size() const
{
    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex:
            return AsAppendable().size();

        case ModifiableAlternativeIndex:
            return AsModifiable().Size();

        case TrimmableAlternativeIndex:
            return AsTrimmable().size() - 1;

        default:
            YT_ABORT();
    }
}

bool TCumulativeStatistics::Empty() const
{
    return Size() == 0;
}

void TCumulativeStatistics::Clear()
{
    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex:
            AsAppendable().clear();
            break;

        case ModifiableAlternativeIndex:
            AsModifiable().Clear();
            break;

        case TrimmableAlternativeIndex:
            AsTrimmable() = TTrimmableCumulativeStatistics(1);
            break;

        default:
            YT_ABORT();
    }
}

int TCumulativeStatistics::LowerBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const
{
    auto comparator = [&member] (const TCumulativeStatisticsEntry& lhs, i64 rhs) {
        return lhs.*member < rhs;
    };

    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex:
            return std::lower_bound(
                AsAppendable().begin(),
                AsAppendable().end(),
                value,
                comparator) - AsAppendable().begin();

        case ModifiableAlternativeIndex:
            return std::max(0, AsModifiable().LowerBound(value, comparator) - 1);

        case TrimmableAlternativeIndex:
            return std::lower_bound(
                AsTrimmable().begin(),
                AsTrimmable().end(),
                value,
                comparator) - AsTrimmable().begin() - 1;

        default:
            YT_ABORT();
    }
}

int TCumulativeStatistics::UpperBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const
{
    auto comparator = [&member] (i64 lhs, const TCumulativeStatisticsEntry& rhs) {
        return lhs < rhs.*member;
    };

    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex:
            return std::upper_bound(
                AsAppendable().begin(),
                AsAppendable().end(),
                value,
                comparator) - AsAppendable().begin();

        case ModifiableAlternativeIndex:
            return std::max(0, AsModifiable().UpperBound(value, comparator) - 1);

        case TrimmableAlternativeIndex:
            return std::upper_bound(
                AsTrimmable().begin(),
                AsTrimmable().end(),
                value,
                comparator) - AsTrimmable().begin() - 1;

        default:
            YT_ABORT();
    }
}

TCumulativeStatisticsEntry TCumulativeStatistics::operator[](int index) const
{
    return GetCurrentSum(index);
}

TCumulativeStatisticsEntry TCumulativeStatistics::GetCurrentSum(int index) const
{
    switch (GetImplementationIndex()) {
        case AppendableAlternativeIndex:
            return AsAppendable()[index];

        case ModifiableAlternativeIndex:
            return AsModifiable().GetCumulativeSum(index + 1);

        case TrimmableAlternativeIndex:
            return AsTrimmable()[index + 1];

        default:
            YT_ABORT();
    }
}

TCumulativeStatisticsEntry TCumulativeStatistics::GetPreviousSum(int index) const
{
    if (index == 0) {
        return IsTrimmable() ? AsTrimmable()[0] : TCumulativeStatisticsEntry{};
    } else {
        return GetCurrentSum(index - 1);
    }
}

TCumulativeStatisticsEntry TCumulativeStatistics::Back() const
{
    YT_VERIFY(!Empty());
    return this->operator[](Size() - 1);
}

void TCumulativeStatistics::TrimFront(int entriesCount)
{
    auto& statistics = AsTrimmable();
    // NB: At least one entry always remains.
    YT_VERIFY(entriesCount <= Size());
    statistics.erase(
        statistics.begin(),
        statistics.begin() + entriesCount);
}

size_t TCumulativeStatistics::GetImplementationIndex() const
{
    return Statistics_.index();
}

bool TCumulativeStatistics::IsAppendable() const
{
    return Statistics_.index() == AppendableAlternativeIndex;
}

bool TCumulativeStatistics::IsModifiable() const
{
    return Statistics_.index() == ModifiableAlternativeIndex;
}

bool TCumulativeStatistics::IsTrimmable() const
{
    return Statistics_.index() == TrimmableAlternativeIndex;
}

TCumulativeStatistics::TAppendableCumulativeStatistics& TCumulativeStatistics::AsAppendable()
{
    YT_VERIFY(IsAppendable());
    return std::get<AppendableAlternativeIndex>(Statistics_);
}

const TCumulativeStatistics::TAppendableCumulativeStatistics& TCumulativeStatistics::AsAppendable() const
{
    YT_VERIFY(IsAppendable());
    return std::get<AppendableAlternativeIndex>(Statistics_);
}

TCumulativeStatistics::TModifiableCumulativeStatistics& TCumulativeStatistics::AsModifiable()
{
    YT_VERIFY(IsModifiable());
    return std::get<ModifiableAlternativeIndex>(Statistics_);
}

const TCumulativeStatistics::TModifiableCumulativeStatistics& TCumulativeStatistics::AsModifiable() const
{
    YT_VERIFY(IsModifiable());
    return std::get<ModifiableAlternativeIndex>(Statistics_);
}

TCumulativeStatistics::TTrimmableCumulativeStatistics& TCumulativeStatistics::AsTrimmable()
{
    YT_VERIFY(IsTrimmable());
    return std::get<TrimmableAlternativeIndex>(Statistics_);
}

const TCumulativeStatistics::TTrimmableCumulativeStatistics& TCumulativeStatistics::AsTrimmable() const
{
    YT_VERIFY(IsTrimmable());
    return std::get<TrimmableAlternativeIndex>(Statistics_);
}

void Serialize(const TCumulativeStatistics& statistics, IYsonConsumer* consumer)
{
    consumer->OnBeginList();
    consumer->OnListItem();
    Serialize(statistics.GetPreviousSum(0), consumer);
    for (int index = 0; index < statistics.Size(); ++index) {
        consumer->OnListItem();
        Serialize(statistics[index], consumer);
    }
    consumer->OnEndList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
