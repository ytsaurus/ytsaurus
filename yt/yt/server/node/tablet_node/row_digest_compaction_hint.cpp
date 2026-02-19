#include "row_digest_compaction_hint.h"
#include "compaction_hint_fetching.h"
#include "sorted_chunk_store.h"
#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

namespace NYT::NTabletNode {

using namespace NProfiling;
using namespace NChunkClient;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TRowDigestFetchPipeline
    : public TCompactionHintFetchPipeline
{
public:
    using TCompactionHintFetchPipeline::TCompactionHintFetchPipeline;

protected:
    virtual NLsm::EStoreCompactionHintKind GetStoreCompactionHintKind() const override
    {
        return NLsm::EStoreCompactionHintKind::VersionedRowDigest;
    }

private:
    virtual void DoFetch() override
    {
        SubscribeWithErrorHandling(
            Store_->GetBackendReaders(EWorkloadCategory::SystemTabletCompaction).ChunkReader->GetMeta(
                /*options*/ {},
                /*partitionTags*/ {},
                /*extentionTags*/ std::vector<int>{TProtoExtensionTag<TVersionedRowDigestExt>::Value}),
            std::bind_front(&TRowDigestFetchPipeline::OnRowDigestMetaReceived, this));
    }

    void OnRowDigestMetaReceived(const TRefCountedChunkMetaPtr& chunkMeta)
    {
        auto rowDigestExt = FindProtoExtension<TVersionedRowDigestExt>(
            chunkMeta->extensions());

        if (!rowDigestExt) {
            OnStoreHasNoHint();
            return;
        }

        auto digest = New<TVersionedRowDigest>();

        ExecuteParse([&] () { FromProto(&*digest, rowDigestExt.value());});

        FinishFetch(std::move(digest));
    }
};

////////////////////////////////////////////////////////////////////////////////

TCompactionHintFetchPipelinePtr CreateRowDigestFetchPipeline(TSortedChunkStore* store)
{
    return New<TRowDigestFetchPipeline>(store);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
