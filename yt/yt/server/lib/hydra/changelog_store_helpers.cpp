#include "changelog_store_helpers.h"

#include "serialize.h"

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////


TChangelogStoreScanResult ScanChangelogStore(
    const std::vector<int>& changelogIds,
    const std::function<TChangelogScanInfo(int changelogId)> scanInfoGetter,
    const std::function<TSharedRef(int changelogId, i64 recordId, bool atPrimaryPath)>& recordReader)
{
    auto sortedChangelogIds = changelogIds;
    std::sort(sortedChangelogIds.begin(), sortedChangelogIds.end(), std::greater<>());

    TChangelogStoreScanResult result;

    for (int id : sortedChangelogIds) {
        if (result.LatestChangelogId != InvalidSegmentId &&
            result.LatestNonemptyChangelogId != InvalidSegmentId)
        {
            // All done.
            break;
        }

        auto scanInfo = scanInfoGetter(id);

        if (id > result.LatestChangelogId) {
            result.LatestChangelogId = id;
            result.LatestChangelogRecordCount = scanInfo.RecordCount;
        }

        if (scanInfo.RecordCount > 0 && id > result.LatestNonemptyChangelogId) {
            result.LatestNonemptyChangelogId = id;
            result.LatestNonemptyChangelogRecordCount = scanInfo.RecordCount;
            result.IsLatestNonemptyChangelogAtPrimaryPath = scanInfo.AtPrimaryPath;
        }
    }

    if (result.LatestNonemptyChangelogId != InvalidSegmentId) {
        YT_VERIFY(result.LatestNonemptyChangelogRecordCount > 0);
        auto record = recordReader(
            result.LatestNonemptyChangelogId,
            result.LatestNonemptyChangelogRecordCount - 1,
            result.IsLatestNonemptyChangelogAtPrimaryPath);

        NHydra::NProto::TMutationHeader header;
        TSharedRef requestData;
        DeserializeMutationRecord(record, &header, &requestData);

        // All mutations have the same term in one changelog.
        // (Of course I am not actually sure in anything at this point, but this actually should be true.)
        result.LastMutationTerm = header.term();
        result.LastMutationSequenceNumber = header.sequence_number();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
