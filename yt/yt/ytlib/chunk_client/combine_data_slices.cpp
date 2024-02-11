#include "combine_data_slices.h"

#include "data_slice_descriptor.h"
#include "data_source.h"

#include <yt/yt/client/chunk_client/read_limit.h>

#include <yt/yt/client/ypath/rich.h>

#include <library/cpp/iterator/enumerate.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

std::vector<NYPath::TRichYPath> CombineDataSlices(
    const TDataSourceDirectoryPtr& dataSourceDirectory,
    std::vector<std::vector<TDataSliceDescriptor>>& slicesByTable,
    const std::optional<std::vector<NYPath::TRichYPath>>& paths)
{
    auto compareAbsoluteReadLimits = [] (const TLegacyReadLimit& lhs, const TLegacyReadLimit& rhs) -> bool {
        YT_VERIFY(lhs.HasRowIndex() == rhs.HasRowIndex());

        if (lhs.HasTabletIndex() && lhs.GetTabletIndex() != rhs.GetTabletIndex()) {
            return lhs.GetTabletIndex() < rhs.GetTabletIndex();
        }

        if (lhs.HasRowIndex() && lhs.GetRowIndex() != rhs.GetRowIndex()) {
            return lhs.GetRowIndex() < rhs.GetRowIndex();
        }

        if (lhs.HasLegacyKey() && rhs.HasLegacyKey()) {
            return lhs.GetLegacyKey() < rhs.GetLegacyKey();
        } else if (lhs.HasLegacyKey()) {
            // rhs is less
            return false;
        } else if (rhs.HasLegacyKey()) {
            // lhs is less
            return true;
        } else {
            // These read limits are effectively equal.
            return false;
        }
    };

    auto canMergeSlices = [] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs, bool versioned, bool sorted) {
        if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
            return false;
        }

        auto lhsUpperLimit = GetAbsoluteUpperReadLimit(lhs, versioned, sorted);
        auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned, sorted);

        // TODO(galtsev): the following upto the return from lambda seem to be equivalent to
        // YT_VERIFY(!compareAbsoluteReadLimits(rhsLowerLimit, lhsUpperLimit));
        // return !compareAbsoluteReadLimits(lhsUpperLimit, rhsLowerLimit);
        // See also: https://a.yandex-team.ru/review/2538526/files/2#comment-3497326

        YT_VERIFY(lhsUpperLimit.HasTabletIndex() == rhsLowerLimit.HasTabletIndex());
        if (lhsUpperLimit.HasTabletIndex() && lhsUpperLimit.GetTabletIndex() != rhsLowerLimit.GetTabletIndex()) {
            return false;
        }

        YT_VERIFY(lhsUpperLimit.HasRowIndex() == rhsLowerLimit.HasRowIndex());
        if (lhsUpperLimit.HasRowIndex() && lhsUpperLimit.GetRowIndex() < rhsLowerLimit.GetRowIndex()) {
            return false;
        }

        if (lhsUpperLimit.HasLegacyKey() != rhsLowerLimit.HasLegacyKey()) {
            return false;
        }

        if (lhsUpperLimit.HasLegacyKey() && lhsUpperLimit.GetLegacyKey() < rhsLowerLimit.GetLegacyKey()) {
            return false;
        }

        return true;
    };

    std::vector<NYPath::TRichYPath> resultPaths;
    resultPaths.reserve(slicesByTable.size());

    YT_VERIFY(dataSourceDirectory->DataSources().size() == slicesByTable.size());
    YT_VERIFY(!paths || paths->size() == slicesByTable.size());

    for (const auto& [tableIndex, dataSource] : Enumerate(dataSourceDirectory->DataSources())) {
        bool versioned = dataSource.GetType() == EDataSourceType::VersionedTable;
        bool sorted = dataSource.Schema()->IsSorted();
        auto& tableSlices = slicesByTable[tableIndex];
        std::sort(
            tableSlices.begin(),
            tableSlices.end(),
            [&] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs) {
                if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
                    return lhs.GetRangeIndex() < rhs.GetRangeIndex();
                }

                auto lhsLowerLimit = GetAbsoluteLowerReadLimit(lhs, versioned, sorted);
                auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned, sorted);

                return compareAbsoluteReadLimits(lhsLowerLimit, rhsLowerLimit);
            });

        std::vector<TReadRange> ranges;
        auto keyLength = dataSource.Schema()->ToComparator().GetLength();

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (!canMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice], versioned, sorted)) {
                    break;
                }
                ++lastSlice;
            }

            auto lowerLimit = GetAbsoluteLowerReadLimit(tableSlices[firstSlice], versioned, sorted);
            auto upperLimit = GetAbsoluteUpperReadLimit(tableSlices[lastSlice - 1], versioned, sorted);
            ranges.emplace_back(
                ReadLimitFromLegacyReadLimit(lowerLimit, /*isUpper*/ false, keyLength),
                ReadLimitFromLegacyReadLimit(upperLimit, /*isUpper*/ true, keyLength));

            firstSlice = lastSlice;
        }

        if (paths) {
            resultPaths.emplace_back((*paths)[tableIndex]);
        } else {
            YT_VERIFY(dataSource.GetPath());
            resultPaths.emplace_back(*dataSource.GetPath());
        }
        auto& path = resultPaths.back();
        path.SetRanges(ranges);
        if (dataSource.GetForeign()) {
            path.SetForeign(true);
        }
        if (dataSource.Columns()) {
            path.SetColumns(*dataSource.Columns());
        }
    }

    YT_VERIFY(resultPaths.size() == slicesByTable.size());

    return resultPaths;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
