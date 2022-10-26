#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Determines some properties of indexed versioned block format.
//! Namely, order of groups in a row and order of columns in a group.
class TIndexedVersionedBlockFormatDetail
{
public:
    explicit TIndexedVersionedBlockFormatDetail(
        const TTableSchemaPtr& schema);

    struct TColumnInfo
    {
        int GroupIndex;
        int ColumnCountInGroup;
        int ColumnIndexInGroup;
    };

    TColumnInfo GetValueColumnInfo(int valueId) const;

    int GetGroupCount() const;

private:
    const int KeyColumnCount_;

    int GroupCount_;
    std::vector<TColumnInfo> ColumnIdToColumnInfo_;
};

////////////////////////////////////////////////////////////////////////////////

class THashTableChunkIndexFormatDetail
{
public:
    static constexpr i64 SectorSize = 4_KB;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
