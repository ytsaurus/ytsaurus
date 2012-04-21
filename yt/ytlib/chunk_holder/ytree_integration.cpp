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

////////////////////////////////////////////////////////////////////////////////

template <class TCollection>
class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TCollection* collection)
        : Collection(collection)
    { }

private:
    TIntrusivePtr<TCollection> Collection;

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        auto chunks = Collection->GetChunks();
        yvector<Stroka> keys;
        keys.reserve(chunks.ysize());
        FOREACH (auto chunk, chunks) {
            keys.push_back(chunk->GetId().ToString());
            if (keys.size() == sizeLimit)
                break;
        }
        return keys;
    }

    virtual size_t GetSize() const
    {
        return Collection->GetChunkCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto id = TChunkId::FromString(key);
        auto chunk = Collection->FindChunk(id);
        if (!chunk) {
            return NULL;
        }

        return IYPathService::FromProducer(BIND([=] (IYsonConsumer* consumer)
            {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("size").Scalar(chunk->GetInfo().size())
                        .Item("location").Scalar(chunk->GetLocation()->GetPath())
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
