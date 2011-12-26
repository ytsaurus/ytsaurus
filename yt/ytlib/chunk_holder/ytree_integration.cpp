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

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        auto chunks = ChunkStore->GetChunks();
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
        return ChunkStore->GetChunks().size(); // TODO(MRoizner): avoid copying
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key) const
    {
        auto id = TChunkId::FromString(key);
        auto chunk = ChunkStore->FindChunk(id);
        if (!chunk) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("size").Scalar(chunk->GetSize())
                        //.Item("location").Scalar(chunk->GetLocation()->GetPath())
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
