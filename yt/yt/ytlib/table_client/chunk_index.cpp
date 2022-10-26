#include "chunk_index.h"

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/small_containers/compact_flat_map.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TIndexedVersionedBlockFormatDetail::TIndexedVersionedBlockFormatDetail(
    const TTableSchemaPtr& schema)
    : KeyColumnCount_(schema->GetKeyColumnCount())
{
    TCompactFlatMap<std::optional<TString>, int, IndexedRowTypicalGroupCount> groupNameToIndex;
    TCompactVector<int, IndexedRowTypicalGroupCount> groupColumnCounts;

    ColumnIdToColumnInfo_.reserve(schema->GetValueColumnCount());

    for (int index = schema->GetKeyColumnCount(); index < schema->GetColumnCount(); ++index) {
        const auto& column = schema->Columns()[index];

        auto it = groupNameToIndex.find(column.Group());
        if (it == groupNameToIndex.end()) {
            it = EmplaceOrCrash(
                groupNameToIndex,
                column.Group(),
                static_cast<int>(groupNameToIndex.size()));

            groupColumnCounts.push_back(0);
        }

        ColumnIdToColumnInfo_.push_back({
            .GroupIndex = it->second,
            .ColumnIndexInGroup = groupColumnCounts[it->second],
        });

        ++groupColumnCounts[it->second];
    }

    for (auto& columnInfo : ColumnIdToColumnInfo_) {
        columnInfo.ColumnCountInGroup = groupColumnCounts[columnInfo.GroupIndex];
    }

    GroupCount_ = std::ssize(groupNameToIndex);
}

TIndexedVersionedBlockFormatDetail::TColumnInfo
TIndexedVersionedBlockFormatDetail::GetValueColumnInfo(int valueId) const
{
    YT_ASSERT(valueId >= KeyColumnCount_);
    return ColumnIdToColumnInfo_[valueId - KeyColumnCount_];
}

int TIndexedVersionedBlockFormatDetail::GetGroupCount() const
{
    return GroupCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
