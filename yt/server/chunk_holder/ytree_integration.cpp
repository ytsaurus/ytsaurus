#include "stdafx.h"
#include "ytree_integration.h"
#include "chunk_store.h"
#include "chunk_cache.h"
#include "chunk.h"
#include "location.h"

#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>

namespace NYT {
namespace NChunkHolder {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <class TCollection>
class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualChunkMap(TCollection* collection)
        : Collection(collection)
    { }

private:
    TIntrusivePtr<TCollection> Collection;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto chunks = Collection->GetChunks();
        std::vector<Stroka> keys;
        keys.reserve(chunks.size());
        FOREACH (auto chunk, chunks) {
            keys.push_back(chunk->GetId().ToString());
            if (keys.size() == sizeLimit)
                break;
        }
        return keys;
    }

    virtual size_t GetSize() const override
    {
        return Collection->GetChunkCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TChunkId::FromString(key);
        auto chunk = Collection->FindChunk(id);
        if (!chunk) {
            return nullptr;
        }

        return IYPathService::FromProducer(BIND([=] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("size").Value(chunk->GetInfo().size())
                    .Item("location").Value(chunk->GetLocation()->GetPath())
                .EndMap();
        }));
    }

};

IYPathServicePtr CreateStoredChunkMapService(TChunkStore* chunkStore)
{
    return New< TVirtualChunkMap<TChunkStore> >(chunkStore);
}

IYPathServicePtr CreateCachedChunkMapService(TChunkCache* chunkCache)
{
    return New< TVirtualChunkMap<TChunkCache> >(chunkCache);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
