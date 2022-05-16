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
    std::vector<std::vector<TDataSliceDescriptor>>& slicesByTable)
{
    auto compareAbsoluteReadLimits = [] (const TLegacyReadLimit& lhs, const TLegacyReadLimit& rhs) -> bool {
        YT_VERIFY(lhs.HasRowIndex() == rhs.HasRowIndex());

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

    auto canMergeSlices = [] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs, bool versioned) {
        if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
            return false;
        }

        auto lhsUpperLimit = GetAbsoluteUpperReadLimit(lhs, versioned);
        auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned);

        // TODO(galtsev): the following upto the return from lambda seem to be equvalent to
        // YT_VERIFY(!compareAbsoluteReadLimits(rhsLowerLimit, lhsUpperLimit));
        // return !compareAbsoluteReadLimits(lhsUpperLimit, rhsLowerLimit);
        // See also: https://a.yandex-team.ru/review/2538526/files/2#comment-3497326

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

    std::vector<NYPath::TRichYPath> paths;

    YT_VERIFY(dataSourceDirectory->DataSources().size() == slicesByTable.size());

    for (const auto& [tableIndex, dataSource] : Enumerate(dataSourceDirectory->DataSources())) {
        bool versioned = dataSource.GetType() == EDataSourceType::VersionedTable;
        auto& tableSlices = slicesByTable[tableIndex];
        std::sort(
            tableSlices.begin(),
            tableSlices.end(),
            [&] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs) {
                if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
                    return lhs.GetRangeIndex() < rhs.GetRangeIndex();
                }

                auto lhsLowerLimit = GetAbsoluteLowerReadLimit(lhs, versioned);
                auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned);

                return compareAbsoluteReadLimits(lhsLowerLimit, rhsLowerLimit);
            });

        std::vector<TReadRange> ranges;
        auto keyLength = dataSource.Schema()->ToComparator().GetLength();

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (!canMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice], versioned)) {
                    break;
                }
                ++lastSlice;
            }

            auto lowerLimit = GetAbsoluteLowerReadLimit(tableSlices[firstSlice], versioned);
            auto upperLimit = GetAbsoluteUpperReadLimit(tableSlices[lastSlice - 1], versioned);
            ranges.emplace_back(
                ReadLimitFromLegacyReadLimit(lowerLimit, /* isUpper */ false, keyLength),
                ReadLimitFromLegacyReadLimit(upperLimit, /* isUpper */ true, keyLength));

            firstSlice = lastSlice;
        }

        // TODO(galtsev): do not check that the ranges is empty; see also https://a.yandex-team.ru/review/2538526/files/2#comment-3497405
        if (!ranges.empty()) {
            auto& path = paths.emplace_back();
            path.SetRanges(ranges);
            if (dataSource.GetForeign()) {
                path.SetForeign(true);
            }
            YT_VERIFY(dataSource.GetPath());
            path.SetPath(*dataSource.GetPath());
        }
    }

    return paths;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
