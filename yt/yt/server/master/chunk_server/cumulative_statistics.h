#pragma once

#include "chunk_tree_statistics.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/core/misc/fenwick_tree.h>

#include <variant>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

struct TCumulativeStatisticsEntry
{
    i64 RowCount = 0;
    i64 ChunkCount = 0;
    i64 DataSize = 0;

    TCumulativeStatisticsEntry();

    explicit TCumulativeStatisticsEntry(const TChunkTreeStatistics& statistics);

    TCumulativeStatisticsEntry(i64 rowCount, i64 chunkCount, i64 dataSize);

    TCumulativeStatisticsEntry operator+(const TCumulativeStatisticsEntry& other) const;
    TCumulativeStatisticsEntry operator-(const TCumulativeStatisticsEntry& other) const;

    bool operator==(const TCumulativeStatisticsEntry& other) const = default;

    void Persist(const NYT::TStreamPersistenceContext& context);
};

void FormatValue(TStringBuilderBase* builder, const TCumulativeStatisticsEntry& entry, TStringBuf spec);

void Serialize(const TCumulativeStatisticsEntry& entry, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! Holds cumulative statistics for children of a chunk tree.
//! Each instance of this class should be declared appendable or modifiable prior to
//! first use via calling `DeclareAppendable()`, `DeclareModifiable()` or `DeclareTrimmable()`.
//!
//! `Appendable` structure stores a prefix sum array and allows quick modifications only
//! at the end.
//!
//! `Modifiable` structure stores a Fenwick tree and allows efficient aggregate modifications
//! at any point at the cost of additional O(log |size|) factor.
//!
//! `Trimmable` structure is the same as `Modifiable` but allows removing entries from both sides.
class TCumulativeStatistics
{
public:
    // Meta interface.
    void DeclareAppendable();
    void DeclareModifiable();
    void DeclareTrimmable();

    void Persist(const NCellMaster::TPersistenceContext& context);

    // Common interface for all kinds.
    void PushBack(const TCumulativeStatisticsEntry& entry);

    void PopBack();

    void Update(int index, const TCumulativeStatisticsEntry& delta);

    i64 Size() const;

    bool Empty() const;

    void Clear();

    int LowerBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const;

    int UpperBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const;

    TCumulativeStatisticsEntry operator[](int index) const;

    TCumulativeStatisticsEntry GetCurrentSum(int index) const;
    TCumulativeStatisticsEntry GetPreviousSum(int index) const;

    TCumulativeStatisticsEntry Back() const;

    // Interface for Trimmable.
    void TrimFront(int entryCount);
    void TrimBack(int entryCount);

private:
    using TAppendableCumulativeStatistics = std::vector<TCumulativeStatisticsEntry>;
    using TModifiableCumulativeStatistics = TFenwickTree<TCumulativeStatisticsEntry>;
    using TTrimmableCumulativeStatistics = std::vector<TCumulativeStatisticsEntry>;

    static constexpr size_t AppendableAlternativeIndex = 0;
    static constexpr size_t ModifiableAlternativeIndex = 1;
    static constexpr size_t TrimmableAlternativeIndex = 2;

    std::variant<
        TAppendableCumulativeStatistics,
        TModifiableCumulativeStatistics,
        TTrimmableCumulativeStatistics
    > Statistics_;

    static_assert(std::is_same_v<
        TAppendableCumulativeStatistics,
        std::decay_t<decltype(std::get<AppendableAlternativeIndex>(Statistics_))>>);

    static_assert(std::is_same_v<
        TModifiableCumulativeStatistics,
        std::decay_t<decltype(std::get<ModifiableAlternativeIndex>(Statistics_))>>);

    static_assert(std::is_same_v<
        TTrimmableCumulativeStatistics,
        std::decay_t<decltype(std::get<TrimmableAlternativeIndex>(Statistics_))>>);

    size_t GetImplementationIndex() const;

    bool IsAppendable() const;
    bool IsModifiable() const;
    bool IsTrimmable() const;

    TAppendableCumulativeStatistics& AsAppendable();
    const TAppendableCumulativeStatistics& AsAppendable() const;

    TModifiableCumulativeStatistics& AsModifiable();
    const TModifiableCumulativeStatistics& AsModifiable() const;

    TTrimmableCumulativeStatistics& AsTrimmable();
    const TTrimmableCumulativeStatistics& AsTrimmable() const;
};

void Serialize(const TCumulativeStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
