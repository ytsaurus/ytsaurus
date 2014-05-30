#pragma once

#include "public.h"
#include "chunk_detail.h"

#include <server/hydra/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunk
{
public:
    TJournalChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkId& id,
        NHydra::IChangelogPtr changelog);

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncError ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        std::vector<TSharedRef>* blocks) override;

    NHydra::IChangelogPtr GetChangelog() const;
    void ReleaseChangelog();

    static TNullable<TChunkDescriptor> TryGetDescriptor(
        const TChunkId& id,
        const Stroka& fileName);

private:
    NHydra::IChangelogPtr Changelog_;

    std::atomic<int> RecordCount_;
    std::atomic<bool> Sealed_;


    void UpdateProperties();

    virtual void EvictFromCache() override;
    virtual TFuture<void> RemoveFiles() override;

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPromise<TError> promise,
        std::vector<TSharedRef>* blocks);

};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

