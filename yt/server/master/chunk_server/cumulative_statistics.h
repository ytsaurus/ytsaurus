#pragma once

#include "chunk_tree_statistics.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/core/misc/fenwick_tree.h>

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

    bool operator==(const TCumulativeStatisticsEntry& other) const;

    void Persist(const NYT::TStreamPersistenceContext& context);
};

TString ToString(const TCumulativeStatisticsEntry& entry);

void Serialize(const TCumulativeStatisticsEntry& entry, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

//! Holds cumulative statistics for children of a chunk tree.
//! Each instance of this class should be declared appendable or modifiable prior to
//! first use via calling `DeclareAppendable()` or `DeclareModifiable()`.
//!
//! `Appendable` structure stores a prefix sum array and allows quick modifications only
//! at the end.
//!
//! `Modifiable` structure stores a Fenwick tree and allows efficient aggregate modifications
//! at any point at the cost of additional O(log |size|) factor.
class TCumulativeStatistics
{
public:
    // Meta interface.
    void DeclareAppendable();

    void DeclareModifiable();

    void Persist(NCellMaster::TPersistenceContext& context);

    // Common interface for appendable and modifiable.
    void PushBack(const TCumulativeStatisticsEntry& entry);

    void PopBack();

    i64 Size() const;

    bool Empty() const;

    void Clear();

    int LowerBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const;

    int UpperBound(i64 value, i64 TCumulativeStatisticsEntry::* member) const;

    TCumulativeStatisticsEntry operator[](int index) const;

    TCumulativeStatisticsEntry Back() const;

    // Interface for appendable.
    void EraseFromFront(int entriesCount);

    // Interface for modifiable.
    void Update(int index, const TCumulativeStatisticsEntry& delta);

private:
    using TAppendableCumulativeStatistics = std::vector<TCumulativeStatisticsEntry>;
    using TModifyableCumulativeStatistics = TFenwickTree<TCumulativeStatisticsEntry>;

    std::variant<TAppendableCumulativeStatistics, TModifyableCumulativeStatistics> Statistics_;

    bool IsAppendable() const;

    bool IsModifiable() const;

    TAppendableCumulativeStatistics& AsAppendable();

    const TAppendableCumulativeStatistics& AsAppendable() const;

    TModifyableCumulativeStatistics& AsModifiable();

    const TModifyableCumulativeStatistics& AsModifiable() const;
};

void Serialize(const TCumulativeStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
