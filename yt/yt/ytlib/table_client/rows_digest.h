#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// XXH3 is much faster than MD5. Based on benchmarks:
// - ~2x faster for typical workloads.
// - ~16x faster for string-heavy data.
class TRowsDigestBuilder final
    : public TNonCopyable
{
public:
    explicit TRowsDigestBuilder(TNameTablePtr nameTable);

    void ProcessRow(TUnversionedRow row) noexcept;

    TRowsDigest GetDigest() const noexcept;

    ~TRowsDigestBuilder();

private:
    struct TColumnInfo
    {
        TColumnInfo(TStringBuf name, ui64 nameHash, ui16 columnNameId) noexcept;

        // String is owned by name table.
        TStringBuf Name;
        ui64 NameHash = 0;
        ui16 ColumnNameId = std::numeric_limits<ui16>::max();

        Y_FORCE_INLINE bool operator<(const TColumnInfo& other) const noexcept;
    };

    const TNameTablePtr NameTable_;
    void* const HasherState_;

    std::vector<TColumnInfo> SortedColumns_;
    std::vector<bool> IsColumnNameIdAdded_;

    // Allocated vector is reused for different rows.
    std::vector<ui16> ValueIdToValueIndexInRow_;

    static constexpr ui32 BatchSize = 2048;
    mutable ui32 BatchOffset_ = 0;
    alignas(64) std::array<std::byte, BatchSize> BatchBuffer_;

    // Y_FORCE_INLINE gives 10-30% performance.
    Y_FORCE_INLINE void RegisterColumn(ui16 columnNameId) noexcept;
    Y_FORCE_INLINE void AppendValue(ui64 nameHash, TUnversionedValue value) noexcept;
    Y_FORCE_INLINE void FlushBatch() const noexcept;
    Y_FORCE_INLINE void AppendToHash(const auto&... args) noexcept;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
