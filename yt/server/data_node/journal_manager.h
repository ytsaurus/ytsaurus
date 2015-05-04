#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <server/hydra/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages journal chunks stored at some specific location.
class TJournalManager
    : public TRefCounted
{
public:
    TJournalManager(
        TDataNodeConfigPtr config,
        TLocation* location,
        NCellNode::TBootstrap* bootstrap);
    ~TJournalManager();

    void Initialize();

    TFuture<NHydra::IChangelogPtr> OpenChangelog(
        const TChunkId& chunkId,
        bool enableMultiplexing);

    TFuture<NHydra::IChangelogPtr> CreateChangelog(
        const TChunkId& chunkId,
        bool enableMultiplexing);

    TFuture<void> RemoveChangelog(TJournalChunkPtr chunk);

    TFuture<void> AppendMultiplexedRecord(
        const TChunkId& chunkId,
        int recordId,
        const TSharedRef& recordData,
        TFuture<void> flushResult);

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TJournalManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

