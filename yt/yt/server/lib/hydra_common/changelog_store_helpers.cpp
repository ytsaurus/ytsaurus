#include "changelog_store_helpers.h"

#include "serialize.h"

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////


TChangelogStoreScanResult ScanChangelogStore(
    const std::vector<TChangelogStoreScanDescriptor>& descriptors,
    const std::function<TSharedRef(int changelogId, i64 recordId)>& recordReader)
{
    TChangelogStoreScanResult result;

    for (const auto& descriptor : descriptors) {
        if (descriptor.Id > result.LatestChangelogId) {
            result.LatestChangelogId = descriptor.Id;
            result.LatestChangelogRecordCount = descriptor.RecordCount;
        }

        if (descriptor.RecordCount > 0 && descriptor.Id > result.LatestNonemptyChangelogId) {
            result.LatestNonemptyChangelogId = descriptor.Id;
            result.LatestNonemptyChangelogRecordCount = descriptor.RecordCount;
        }
    }

    if (result.LatestNonemptyChangelogId != InvalidTerm) {
        auto record = recordReader(result.LatestNonemptyChangelogId, result.LatestNonemptyChangelogRecordCount - 1);

        NHydra::NProto::TMutationHeader header;
        TSharedRef requestData;
        DeserializeMutationRecord(record, &header, &requestData);

        // All mutations have the same term in one changelog.
        // (Of course I am not actualy sure in anything at this point, but this actually should be true.)
        result.LastMutationTerm = header.term();
        result.LastMutationSequenceNumber = header.sequence_number();
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
