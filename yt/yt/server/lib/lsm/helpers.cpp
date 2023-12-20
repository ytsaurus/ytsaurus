#include "helpers.h"
#include "store.h"

#include <yt/yt/server/lib/tablet_node/private.h>
#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/table_client/versioned_row_digest.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/library/quantile_digest/quantile_digest.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NLsm {

using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = NTabletNode::TabletNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TRowDigestUpcomingCompactionInfo GetUpcomingCompactionInfo(
    TStoreId storeId,
    const TTableMountConfigPtr& mountConfig,
    const TVersionedRowDigest& digest)
{
    const auto& allButLastDigest = digest.AllButLastTimestampDigest;
    const auto& lastDigest = digest.LastTimestampDigest;
    const auto& earliestNthTimestamp = digest.EarliestNthTimestamp;

    auto minDataTtl = mountConfig->MinDataTtl;
    auto maxDataTtl = mountConfig->MaxDataTtl;
    int minDataVersions = mountConfig->MinDataVersions;
    int maxDataVersions = mountConfig->MaxDataVersions;
    int maxTimestampsPerValue = mountConfig->RowDigestCompaction->MaxTimestampsPerValue;
    double maxObsoleteTimestampRatio = mountConfig->RowDigestCompaction->MaxObsoleteTimestampRatio;

    i64 totalCount = allButLastDigest->GetCount() + lastDigest->GetCount();

    TRowDigestUpcomingCompactionInfo result;

    // Check if there is timestamp when a certain ratio of data will be compacted.
    if (minDataVersions == 1) {
        if (allButLastDigest->GetCount() > 0) {
            if (double sufficientQuantile = (totalCount * maxObsoleteTimestampRatio) / allButLastDigest->GetCount();
                sufficientQuantile <= 1)
            {
                TInstant compactionTimestamp = TInstant::Seconds(allButLastDigest->GetQuantile(
                    sufficientQuantile)) + minDataTtl;
                YT_LOG_DEBUG("Found upcoming compaction timestamp only using AllButLastTimestampDigest "
                    "(StoreId: %v, Timestamp: %v, Reason: %v)",
                    storeId,
                    compactionTimestamp,
                    EStoreCompactionReason::TtlCleanupExpected);

                result = {
                    .Reason = EStoreCompactionReason::TtlCleanupExpected,
                    .Timestamp = compactionTimestamp,
                };
            }
        }
    } else if (totalCount > 0) {
        auto lastTtl = maxDataVersions == 0
            ? minDataTtl
            : maxDataTtl;

        auto left = TInstant::Seconds(std::min(
            allButLastDigest->GetQuantile(0),
            lastDigest->GetQuantile(0)));
        auto right = TInstant::Seconds(std::max(
            allButLastDigest->GetQuantile(maxObsoleteTimestampRatio),
            lastDigest->GetQuantile(maxObsoleteTimestampRatio))) + lastTtl;

        while (right - left > CompactionTimestampAccuracy) {
            auto mid = left + (right - left) / 2;

            double currentRatio =
                (allButLastDigest->GetRank((mid - minDataTtl).Seconds()) * allButLastDigest->GetCount() +
                lastDigest->GetRank((mid - lastTtl).Seconds()) * lastDigest->GetCount()) / totalCount;

            if (currentRatio >= maxObsoleteTimestampRatio) {
                right = mid;
            } else {
                left = mid;
            }
        }

        YT_LOG_DEBUG("Found upcoming compaction timestamp (StoreId: %v, Timestamp: %v, Reason: %v)",
            storeId,
            right,
            EStoreCompactionReason::TtlCleanupExpected);

        result = {
            .Reason = EStoreCompactionReason::TtlCleanupExpected,
            .Timestamp = right,
        };
    }

    // Check if there is a value with many old timestamps.
    if (int index = 32 - std::countl_zero(std::max<ui32>(maxTimestampsPerValue - 1, 1));
        index < ssize(earliestNthTimestamp))
    {
        auto compactionTimestamp = TInstant::Seconds(earliestNthTimestamp[index]) + minDataTtl;
        YT_LOG_DEBUG("Found upcoming compaction timestamp "
            "(StoreId: %v, TimestampIndex: %v, EarliestNthTimestamp: %v, Timestamp: %v, Reason: %v)",
            storeId,
            index,
            earliestNthTimestamp[index],
            compactionTimestamp,
            EStoreCompactionReason::TooManyTimestamps);

        if (result.Reason == EStoreCompactionReason::None || result.Timestamp > compactionTimestamp) {
            result = {
                .Reason = EStoreCompactionReason::TooManyTimestamps,
                .Timestamp = compactionTimestamp,
            };
        }
    }

    if (result.Reason == EStoreCompactionReason::None) {
        YT_LOG_DEBUG("No timestamp for upcoming compaction (StoreId: %v)",
            storeId);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
