#include "output_order.h"

#include <yt/ytlib/chunk_client/input_chunk.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;

TInputChunkPtr TOutputOrder::TEntry::GetTeleportChunk() const
{
    return Content_.As<TInputChunkPtr>();
}

TOutputOrder::TCookie TOutputOrder::TEntry::GetCookie() const
{
    return Content_.As<TCookie>();
}

bool TOutputOrder::TEntry::IsCookie() const
{
    return Content_.Is<TCookie>();
}

bool TOutputOrder::TEntry::IsTeleportChunk() const
{
    return Content_.Is<TInputChunkPtr>();
}

TOutputOrder::TEntry::TEntry(TInputChunkPtr teleportChunk)
    : Content_(teleportChunk)
{ }

TOutputOrder::TEntry::TEntry(TCookie cookie)
    : Content_(cookie)
{ }

TOutputOrder::TEntry::TEntry()
    : Content_(-1)
{ }

void TOutputOrder::TEntry::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Content_);
}

////////////////////////////////////////////////////////////////////////////////

void TOutputOrder::Push(TOutputOrder::TEntry entry)
{
    if (Pool_.empty()) {
        // Initial push, we should set Begin_ and BeforeEnd_.
        Pool_.emplace_back(std::move(entry));
        NextPosition_.emplace_back(-1);
        CurrentPosition_ = 0;
    } else {
        int newPosition = Pool_.size();
        Pool_.emplace_back(std::move(entry));
        NextPosition_.emplace_back(NextPosition_[CurrentPosition_]);
        NextPosition_[CurrentPosition_] = newPosition;
        CurrentPosition_ = newPosition;
    }
    if (Pool_.back().IsCookie()) {
        if (CookieToPosition_.size() <= Pool_.back().GetCookie()) {
            CookieToPosition_.resize(Pool_.back().GetCookie() + 1);
        }
        CookieToPosition_[Pool_.back().GetCookie()] = CurrentPosition_;
    } else {
        TeleportChunkToPosition_[Pool_.back().GetTeleportChunk()] = CurrentPosition_;
    }
}

void TOutputOrder::SeekCookie(TCookie cookie)
{
    Y_ASSERT(0 <= cookie && cookie < CookieToPosition_.size());
    CurrentPosition_ = CookieToPosition_[cookie];
}

int TOutputOrder::GetSize() const
{
    return Pool_.size();
}

std::vector<TChunkTreeId> TOutputOrder::ArrangeOutputChunkTrees(
    std::vector<std::pair<TOutputOrder::TEntry, TChunkTreeId>> chunkTrees)
{
    if (Pool_.empty()) {
        YCHECK(chunkTrees.empty());
        return {};
    }
    
    std::vector<TChunkTreeId> chunkTreeByPosition;
    chunkTreeByPosition.resize(Pool_.size());
    for (const auto& pair : chunkTrees) {
        int position = -1;
        if (pair.first.IsCookie()) {
            Y_ASSERT(0 <= pair.first.GetCookie() && pair.first.GetCookie() < CookieToPosition_.size());
            position = CookieToPosition_[pair.first.GetCookie()];
        } else {
            auto iterator = TeleportChunkToPosition_.find(pair.first.GetTeleportChunk());
            Y_ASSERT(iterator != TeleportChunkToPosition_.end());
            position = iterator->second;
        }
        chunkTreeByPosition[position] = pair.second;
    }

    std::vector<TChunkTreeId> arrangedChunkTrees;
    arrangedChunkTrees.reserve(Pool_.size());
    for (int position = 0; position != -1; position = NextPosition_[position]) {
        if (chunkTreeByPosition[position]) {
            arrangedChunkTrees.emplace_back(chunkTreeByPosition[position]);
        }
    }

    return arrangedChunkTrees;
}

std::vector<TOutputOrder::TEntry> TOutputOrder::ToEntryVector() const
{
    std::vector<TEntry> entries;
    entries.reserve(Pool_.size());
    for (int position = 0; position != -1; position = NextPosition_[position]) {
        entries.emplace_back(Pool_[position]);
    }
    return entries;
}

void TOutputOrder::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;

    Persist<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>(context, TeleportChunkToPosition_);
    Persist(context, CookieToPosition_);
    Persist(context, Pool_);
    Persist(context, NextPosition_);
    Persist(context, CurrentPosition_);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TOutputOrder::TEntry& entry) {
    using NYT::ToString;

    if (entry.IsCookie()) {
        return "cookie@" + ToString(entry.GetCookie());
    } else {
        return "chunk@" + ToString(entry.GetTeleportChunk());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
