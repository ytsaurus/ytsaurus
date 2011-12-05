#include "stdafx.h"
#include "ytree_integration.h"

#include "../ytree/virtual.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NChunkHolder {

using namespace NYTree;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TChunkStore* chunkStore)
        : ChunkStore(chunkStore)
    { }

private:
    TChunkStore::TPtr ChunkStore;

    virtual yvector<Stroka> GetKeys()
    {
        auto chunks = ChunkStore->GetChunks();
        yvector<Stroka> keys;
        keys.reserve(chunks.ysize());
        FOREACH (auto chunk, chunks) {
            keys.push_back(chunk->GetId().ToString());
        }
        return keys;
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TChunkId::FromString(key);
        auto chunk = ChunkStore->FindChunk(id);
        if (~chunk == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                const auto& info = chunk->Info();
                // TODO: location id?
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("type").Scalar(EChunkType(info.GetAttributes().GetType()).ToString())
                        .Item("size").Scalar(info.GetSize())
                        .Item("block_count").Scalar(info.BlocksSize())
                    .EndMap();
            }));
    }

};

IYPathService::TPtr CreateChunkMapService(TChunkStore* chunkStore)
{
    return New<TVirtualChunkMap>(chunkStore);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
