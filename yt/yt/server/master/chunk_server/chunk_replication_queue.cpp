#include "chunk_replication_queue.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NChunkServer {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void TChunkReplicationQueue::TChunkReplicationQueue::Add(
    TChunkIdWithIndex chunkIdWithIndex,
    int targetMediumIndex)
{
    auto [it, inserted] = Queue_.emplace(chunkIdWithIndex, TMediumSet());
    if (inserted) {
        RandomIterator_ = it;
    }
    it->second.set(targetMediumIndex);
}

void TChunkReplicationQueue::TChunkReplicationQueue::Erase(
    TChunkIdWithIndex chunkIdWithIndex,
    int targetMediumIndex)
{
    auto it = Queue_.find(chunkIdWithIndex);
    YT_VERIFY(it != Queue_.end());
    it->second.reset(targetMediumIndex);

    if (it->second.none()) {
        Erase(it);
    }
}

void TChunkReplicationQueue::TChunkReplicationQueue::Erase(TChunkIdWithIndex chunkIdWithIndex)
{
    Erase(Queue_.find(chunkIdWithIndex));
}

void TChunkReplicationQueue::TChunkReplicationQueue::Erase(TIterator it)
{
    if (it == Queue_.end()) {
        return;
    }

    if (RandomIterator_ == it) {
        AdvanceRandomIterator();
    }
    Queue_.erase(it);
    if (Queue_.empty()) {
        RandomIterator_ = Queue_.end();
    }
}

TChunkReplicationQueue::TIterator TChunkReplicationQueue::PickRandomChunk()
{
    auto it = RandomIterator_;
    AdvanceRandomIterator();
    return it;
}

TChunkReplicationQueue::TIterator TChunkReplicationQueue::begin()
{
    return Queue_.begin();
}

TChunkReplicationQueue::TIterator TChunkReplicationQueue::end()
{
    return Queue_.end();
}

TChunkReplicationQueue::TIterator TChunkReplicationQueue::find(NChunkClient::TChunkIdWithIndex chunkIdWithIndex)
{
    return Queue_.find(chunkIdWithIndex);
}

size_t TChunkReplicationQueue::size() const
{
    return Queue_.size();
}

bool TChunkReplicationQueue::empty() const
{
    return Queue_.empty();
}

void TChunkReplicationQueue::clear()
{
    Queue_.clear();
    RandomIterator_ = Queue_.end();
}

void TChunkReplicationQueue::Shrink()
{
    if (Queue_.empty()) {
        ShrinkHashTable(Queue_);
        RandomIterator_ = Queue_.end();
        return;
    }

    auto randomChunk = RandomIterator_->first;
    ShrinkHashTable(Queue_);
    RandomIterator_ = Queue_.find(randomChunk);
    YT_VERIFY(RandomIterator_ != Queue_.end());
}

void TChunkReplicationQueue::AdvanceRandomIterator()
{
    if (RandomIterator_ == Queue_.end()) {
        return;
    }

    ++RandomIterator_;
    if (RandomIterator_ == Queue_.end()) {
        RandomIterator_ = Queue_.begin();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
