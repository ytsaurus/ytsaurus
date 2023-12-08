#include "public.h"

#include <yt/yt/client/chunk_client/chunk_replica.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TChunkReplicationQueue
{
public:
    using TIterator = THashMap<NChunkClient::TChunkIdWithIndex, TMediumSet>::iterator;

    void Add(NChunkClient::TChunkIdWithIndex chunkIdWithIndex, int targetMediumIndex);
    void Erase(NChunkClient::TChunkIdWithIndex chunkIdWithIndex, int targetMediumIndex);
    void Erase(NChunkClient::TChunkIdWithIndex chunkIdWithIndex);
    void Erase(TIterator iterator);

    TIterator PickRandomChunk();

    TIterator begin();
    TIterator end();

    TIterator find(NChunkClient::TChunkIdWithIndex chunkIdWithIndex);

    [[nodiscard]] size_t size() const;

    [[nodiscard]] bool empty() const;

    void clear();

    void Shrink();

private:
    //! Key:
    //!   Encodes chunk and one of its parts (for erasure chunks only, others use GenericChunkReplicaIndex).
    //! Value:
    //!   Indicates media where acting as replication targets for this chunk.
    THashMap<NChunkClient::TChunkIdWithIndex, TMediumSet> Queue_;
    TIterator RandomIterator_ = Queue_.end();

    void AdvanceRandomIterator();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
