#include "output_order.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using namespace NChunkClient;

TInputChunkPtr TOutputOrder::TEntry::GetTeleportChunk() const
{
    return std::get<TInputChunkPtr>(Content_);
}

TOutputOrder::TCookie TOutputOrder::TEntry::GetCookie() const
{
    return std::get<TCookie>(Content_);
}

bool TOutputOrder::TEntry::IsCookie() const
{
    return std::holds_alternative<TCookie>(Content_);
}

bool TOutputOrder::TEntry::IsTeleportChunk() const
{
    return std::holds_alternative<TInputChunkPtr>(Content_);
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

void TOutputOrder::TEntry::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, Content_);
}

bool TOutputOrder::TEntry::operator == (const TOutputOrder::TEntry& other) const
{
    return Content_ == other.Content_;
}

PHOENIX_DEFINE_TYPE(TOutputOrder::TEntry);

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
        if (std::ssize(CookieToPosition_) <= Pool_.back().GetCookie()) {
            CookieToPosition_.resize(Pool_.back().GetCookie() + 1);
        }
        CookieToPosition_[Pool_.back().GetCookie()] = CurrentPosition_;
    } else {
        TeleportChunkToPosition_[Pool_.back().GetTeleportChunk()] = CurrentPosition_;
    }
}

void TOutputOrder::SeekCookie(TCookie cookie)
{
    YT_ASSERT(0 <= cookie && cookie < std::ssize(CookieToPosition_));
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
        YT_VERIFY(chunkTrees.empty());
        return {};
    }

    std::vector<TChunkTreeId> chunkTreeIdByPosition;
    chunkTreeIdByPosition.resize(Pool_.size());
    for (const auto& [entry, chunkTreeId] : chunkTrees) {
        int position = -1;
        if (entry.IsCookie()) {
            YT_ASSERT(0 <= entry.GetCookie() && entry.GetCookie() < std::ssize(CookieToPosition_));
            position = CookieToPosition_[entry.GetCookie()];
        } else {
            auto iterator = TeleportChunkToPosition_.find(entry.GetTeleportChunk());
            YT_ASSERT(iterator != TeleportChunkToPosition_.end());
            position = iterator->second;
        }
        chunkTreeIdByPosition[position] = chunkTreeId;
    }

    std::vector<TChunkTreeId> arrangedChunkTrees;
    arrangedChunkTrees.reserve(Pool_.size());
    for (int position = 0; position != -1; position = NextPosition_[position]) {
        if (chunkTreeIdByPosition[position]) {
            arrangedChunkTrees.emplace_back(chunkTreeIdByPosition[position]);
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

void TOutputOrder::RegisterMetadata(auto&& registrar)
{
    PHOENIX_REGISTER_FIELD(1, TeleportChunkToPosition_,
        .template Serializer<TMapSerializer<TDefaultSerializer, TDefaultSerializer, TUnsortedTag>>());
    PHOENIX_REGISTER_FIELD(2, CookieToPosition_);
    PHOENIX_REGISTER_FIELD(3, Pool_);
    PHOENIX_REGISTER_FIELD(4, NextPosition_);
    PHOENIX_REGISTER_FIELD(5, CurrentPosition_);
}

PHOENIX_DEFINE_TYPE(TOutputOrder);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TOutputOrder::TEntry& entry, TStringBuf /*spec*/)
{
    if (entry.IsCookie()) {
        builder->AppendFormat("cookie@", entry.GetCookie());
    } else {
        builder->AppendFormat("chunk@", entry.GetTeleportChunk());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
