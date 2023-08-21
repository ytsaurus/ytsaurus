#include "ytree_integration.h"
#include "ally_replica_manager.h"
#include "artifact.h"
#include "chunk.h"
#include "chunk_store.h"
#include "journal_chunk.h"
#include "location.h"

#include <yt/yt/server/node/exec_node/chunk_cache.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NDataNode {

using namespace NExecNode;
using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

template <class TCollection>
class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(
        TIntrusivePtr<TCollection> collection,
        IAllyReplicaManagerPtr allyReplicaManager)
        : Collection_(std::move(collection))
        , AllyReplicaManager_(std::move(allyReplicaManager))
    { }

private:
    const TIntrusivePtr<TCollection> Collection_;
    const IAllyReplicaManagerPtr AllyReplicaManager_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        auto chunks = Collection_->GetChunks();
        std::vector<TString> keys;
        keys.reserve(chunks.size());
        for (auto chunk : chunks) {
            keys.push_back(ToString(chunk->GetId()));
            if (std::ssize(keys) == sizeLimit)
                break;
        }
        return keys;
    }

    i64 GetSize() const override
    {
        return Collection_->GetChunkCount();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto id = TChunkId::FromString(key);
        auto chunk = Collection_->FindChunk(id);
        if (!chunk) {
            return nullptr;
        }

        return IYPathService::FromProducer(BIND([=, this, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
            TChunkReadOptions options;
            options.ChunkReaderStatistics = New<TChunkReaderStatistics>();
            auto chunkMeta = NYT::NConcurrency::WaitFor(chunk->ReadMeta(options))
                .ValueOrThrow();
            auto blocksExt = FindProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta->extensions());
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("disk_space").Value(chunk->GetInfo().disk_space())
                    .Item("location").Value(chunk->GetLocation()->GetPath())
                    .Item("artifact").Value(IsArtifactChunkId(chunk->GetId()))
                    .DoIf(blocksExt.has_value(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("block_count").Value(blocksExt->blocks_size());
                    })
                    .DoIf(chunk->IsJournalChunk(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("flushed_row_count").Value(chunk->AsJournalChunk()->GetFlushedRowCount());
                    })
                    .DoIf(static_cast<bool>(AllyReplicaManager_), [&] (TFluentMap fluent) {
                        AllyReplicaManager_->BuildChunkOrchidYson(fluent, chunk->GetId());
                    })
                .EndMap();
        }));
    }
};

IYPathServicePtr CreateStoredChunkMapService(
    TChunkStorePtr chunkStore,
    IAllyReplicaManagerPtr allyReplicaManager)
{
    return New<TVirtualChunkMap<TChunkStore>>(
        std::move(chunkStore),
        std::move(allyReplicaManager));
}

IYPathServicePtr CreateCachedChunkMapService(TChunkCachePtr chunkCache)
{
    return New<TVirtualChunkMap<TChunkCache>>(
        std::move(chunkCache),
        /*allyReplicaManager*/ nullptr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
